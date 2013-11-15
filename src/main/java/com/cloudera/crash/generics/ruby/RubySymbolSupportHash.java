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

package com.cloudera.crash.generics.ruby;

import org.jruby.Ruby;
import org.jruby.RubyHash;
import org.jruby.RubyString;
import org.jruby.RubySymbol;
import org.jruby.runtime.builtin.IRubyObject;

public class RubySymbolSupportHash extends RubyHash {
  private RubySymbolSupportHash(Ruby runtime) {
    super(runtime);
  }

  public static RubySymbolSupportHash newSymbolSupportHash(Ruby runtime) {
    return new RubySymbolSupportHash(runtime);
  }

  @Override
  protected IRubyObject internalGet(IRubyObject key) {
    IRubyObject value = super.internalGet(key);
    if (value != null) {
      return value;
    } else {
      return super.internalGet(coerce(key));
    }
  }

  private IRubyObject coerce(IRubyObject key) {
    if (key instanceof RubyString) {
      return RubySymbol.newSymbol(getRuntime(), key.asJavaString());
    } else {
      return key.asString();
    }
  }
}
