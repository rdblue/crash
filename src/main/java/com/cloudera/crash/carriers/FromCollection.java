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

public abstract class FromCollection<S, T> extends Carrier<S, T> {
  protected FromCollection(String name, Main.Script script) {
    super(name, script);
  }

  @Override
  public final void process(S input) {
    work(input);
  }

  public abstract void work(S input);
}
