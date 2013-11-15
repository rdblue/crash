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

package com.cloudera.crash.utils;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

/**
 * Translates from a PCollection to a PTable.
 *
 * If the PCollection contained Pairs, then the resulting table is made from
 * those Pairs. Otherwise, the table is created with Pairs containing each
 * object from the PCollection as a key and null values.
 *
 * @param <I>
 */
public class ToTableShim<I> extends DoFn<I, Pair> {

  @Override
  @SuppressWarnings("unchecked")
  public void process(I input, Emitter<Pair> emitter) {
    if (input instanceof Pair) {
      emitter.emit((Pair) input);
    } else {
      emitter.emit(Pair.of(input, null));
    }
  }
}
