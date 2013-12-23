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

package com.cloudera.crash.carriers;

import com.cloudera.crash.Main;
import com.google.common.base.Preconditions;
import org.apache.crunch.CombineFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.io.ObjectStreamException;

public abstract class Combiner<K, V> extends CombineFn<K, V> {
  private final String name;
  private final Main.Script script;

  private transient Emitter<Pair<K, V>> emitter;

  protected Combiner(String name, Main.Script script) {
    this.name = name;
    this.script = script;
  }

  @Override
  public final void process(Pair<K, Iterable<V>> input, Emitter<Pair<K, V>> emitter) {
    this.emitter = emitter;
    work(input.first(), wrap(input.second()));
  }

  public void emit(K key, V value) {
    Preconditions.checkArgument(emitter != null,
        "Cannot call emit outside of processing");
    emitter.emit(Pair.of(key, value));
  }

  @SuppressWarnings("unchecked")
  public void emit(Object value) {
    Preconditions.checkArgument(emitter != null,
        "Cannot call emit outside of processing");
    emitter.emit((Pair<K, V>) value);
  }

  public Iterable<V> wrap(Iterable<V> iterable) {
    // TODO: wrap the iterable in something more friendly
    return iterable;
  }

  public final void increment(Object counter) {
    increment(counter, 1);
  }
  public final void increment(Object counter, long amount) {
    increment(script.getName(), counter.toString(), amount);
  }

  public final void increment(Object group, Object counter) {
    increment(group, counter, 1);
  }

  public final void increment(Object group, Object counter, long amount) {
    increment(group.toString(), counter.toString(), amount);
  }

  public abstract void work(K key, Iterable<V> values);

  protected Object writeReplace() throws ObjectStreamException {
    return new StandIn(name, script);
  }
}
