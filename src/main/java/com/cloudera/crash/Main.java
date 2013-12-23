/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the license at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.crash;

import com.cloudera.crash.generics.python.PyReaderWriterFactory;
import com.cloudera.crash.generics.ruby.RubyReaderWriterFactory;
import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.crunch.DoFn;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.types.avro.AvroMode;
import org.apache.crunch.util.CrunchTool;
import org.apache.crunch.util.DistCache;
import org.apache.hadoop.util.ToolRunner;
import org.python.core.Py;
import org.python.core.PyObject;
import org.python.core.PySystemState;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class Main extends CrunchTool {

  public static final String IN_MEMORY_PROP = "mem.pipeline";

  public static class Script implements Serializable {
    /*
     * This cache is used to avoid evaluating the script multiple times when
     * this is deserialized. There can be multiple copies of this object, but
     * they will all go through this static cache to eval the analytic bytes.
     */
    private static Cache<String, Analytic> analytics = CacheBuilder.newBuilder().build();

    private String name;
    private byte[] bytes;

    private transient Pipeline pipeline = null;
    private transient Analytic analytic = null;

    public Script(String name, byte[] bytes) {
      this.name = name;
      this.bytes = bytes;
    }

    public String getName() {
      return name;
    }

    public void setPipeline(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    public Pipeline getPipeline() {
      if (pipeline != null) {
        return pipeline;
      } else {
        return new MRPipeline(Main.class);
      }
    }

    public Analytic getAnalytic() {
      if (analytic == null) {
        try {
          this.analytic = analytics.get(name, new Callable<Analytic>() {
            @Override
            public Analytic call() throws ScriptException {
              return eval(getScript());
            }
          });
        } catch (ExecutionException ex) {
          throw Throwables.propagate(ex);
        }
      }
      return analytic;
    }

    public <S, T> DoFn<S, T> getStage(String name) {
      return getAnalytic().getStage(name);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("name", name)
          .toString();
    }

    private Script getScript() {
      return this;
    }

    public static Analytic eval(Script script) throws ScriptException {
      System.err.println("Evaluating script:\n" + Charset.forName("utf-8").decode(ByteBuffer.wrap(script.bytes)));
      if (script.name.endsWith(".rb")) {
        return rubyEval(script);
      } else if (script.name.endsWith(".py")) {
        return pyEval(script);
      } else {
        throw new RuntimeException("Unknown extension: cannot evaluate \"" + script.name + "\"");
      }
    }

    public static Analytic rubyEval(Script script) throws ScriptException {
      // use the same runtime and variable map for all ScriptingContainers
      System.setProperty("org.jruby.embed.localcontext.scope", "singleton");
      System.setProperty("org.jruby.embed.compat.version", "JRuby1.9");
      // keep local variables around between calls to eval
      System.setProperty("org.jruby.embed.localvariable.behavior", "persistent");
      // make sure object hashing is consistent across all JVM instances, PR #640
      System.setProperty("jruby.consistent.hashing", "true");

      // use ruby custom avro data
      AvroMode.GENERIC.override(new RubyReaderWriterFactory());

      ScriptEngine engine = new ScriptEngineManager().getEngineByName("jruby");
      Bindings bindings = new SimpleBindings();
      bindings.put("$SCRIPT", script);
      return (Analytic) engine.eval(
          new InputStreamReader(new ByteArrayInputStream(script.bytes)),
          bindings);
    }

    public static Analytic pyEval(Script script) throws ScriptException {
      // a hack for python to pass the analytic back
      List<Analytic> analytics = Lists.newArrayListWithCapacity(1);
      PySystemState engineSys = new PySystemState();
      PyObject builtins = engineSys.getBuiltins();
      builtins.__setitem__("_script", Py.java2py(script));
      builtins.__setitem__("_analytics", Py.java2py(analytics));
      Py.setSystemState(engineSys);

      // use ruby custom avro data
      AvroMode.GENERIC.override(new PyReaderWriterFactory());

      ScriptEngine engine = new ScriptEngineManager().getEngineByName("python");
      Bindings bindings = new SimpleBindings();
      engine.eval(
          new InputStreamReader(new ByteArrayInputStream(script.bytes)),
          bindings);

      return analytics.get(0);
    }
  }

  public Main(boolean inMemory) {
    super(inMemory);
  }

  public static final Set<String> FLAGS = Sets.newHashSet("verbose");

  @Override
  public int run(String[] args) throws Exception {
    // generic options parsing...
    HashMultimap<String, String> options = HashMultimap.create();
    List<String> targets = Lists.newArrayList();
    PeekingIterator <String> strings = Iterators.peekingIterator(Iterators.forArray(args));
    while (strings.hasNext()) {
      final String arg = strings.next();
      if (arg.startsWith("--")) {
        final String option = arg.substring(2);
        if (FLAGS.contains(option) || strings.peek().startsWith("--")) {
          options.put(option, "true");
        } else {
          options.putAll(option, Splitter.on(',').split(strings.next()));
        }
      } else {
        targets.add(arg);
      }
    }

    // add directories to the distributed cache
    // -libjars doesn't seem to work with vendor/
    if (options.containsKey("vendor")) {
      for (String path : options.get("vendor")) {
        File file = new File(path);
        if (file.isDirectory()) {
          DistCache.addJarDirToDistributedCache(getConf(), file);
        } else if (file.isFile()) {
          DistCache.addJarToDistributedCache(getConf(), file);
        }
      }
    }

    if (targets.isEmpty()) {
      // TODO: usage
      System.err.println("No script provided!");
      return 1;
    }
    final String scriptName = targets.get(0);

    Script script = new Script(scriptName,
        Files.toByteArray(new File(scriptName)));
    script.setPipeline(getPipeline());
    script.getAnalytic();

    PipelineResult result = this.run();

    return result.succeeded() ? 0 : 1;
  }

  public static void main(String... args) throws Exception {
    boolean inMemory = (System.getProperty(IN_MEMORY_PROP) != null);
    System.exit(ToolRunner.run(new Main(inMemory), args));
  }
}
