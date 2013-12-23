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
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import java.io.ObjectStreamException;

public abstract class Carrier<S, T> extends DoFn<S, T> {

  private final String name;
  private final Main.Script script;

  private transient Emitter<T> emitter;

  protected Carrier(String name, Main.Script script) {
    this.name = name;
    this.script = script;
  }

  @Override
  public final void process(S input, Emitter<T> emitter) {
    this.emitter = emitter;
    process(input);
  }

  @SuppressWarnings("unchecked")
  public final void emit(Object key, Object value) {
    Preconditions.checkState(emitter != null,
        "Cannot call emit outside of processing");
    emit((T) Pair.of(key, value));
  }

  public final void emit(T output) {
    Preconditions.checkState(emitter != null,
        "Cannot call emit outside of processing");
    emitter.emit(output);
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

  public abstract void process(S input);

  protected Object writeReplace() throws ObjectStreamException {
    return new StandIn(name, script);
  }

}
