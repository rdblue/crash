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

import com.cloudera.crash.generics.DataFactory;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.jcodings.specific.UTF8Encoding;
import org.jruby.Ruby;
import org.jruby.RubyArray;
import org.jruby.RubyClass;
import org.jruby.RubyHash;
import org.jruby.RubyString;
import org.jruby.RubyStruct;
import org.jruby.RubySymbol;
import org.jruby.runtime.Block;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.util.ByteList;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class RubyDataFactory implements DataFactory
    <RubyStruct, RubyHash, RubyArray, RubyString, RubySymbol> {
  private static final LoadingCache<Ruby, RubyDataFactory> dataCache =
      CacheBuilder.newBuilder().build(
          CacheLoader.from(new Function<Ruby, RubyDataFactory>() {
            @Override
            public RubyDataFactory apply(@Nullable Ruby runtime) {
              if (runtime != null) {
                return new RubyDataFactory(runtime);
              } else {
                return null;
              }
            }
          }));

  public static RubyDataFactory get(Ruby runtime) {
    try {
      return dataCache.get(runtime);
    } catch (ExecutionException ex) {
      throw Throwables.propagate(ex);
    }
  }

  private final Cache<String, RubyClass> structClassCache =
      CacheBuilder.newBuilder().build();

  private final Ruby runtime;

  private RubyDataFactory(Ruby runtime) {
    this.runtime = runtime;
  }

  @Override
  public RubyStruct createRecord(String name, List<String> fields) {
    RubyClass structClass = structClassCache.getIfPresent(name);
    if (structClass == null) {
      structClass = getStructClass(runtime, name, fields);
      structClassCache.put(name, structClass);
    }
    return RubyStruct.newStruct(structClass, Block.NULL_BLOCK);
  }

  @Override
  public RubyString createString(String javaString) {
    return RubyString.newString(runtime, javaString);
  }

  @Override
  public RubyString createString(ByteBuffer utf8) {
    // create a ByteList backed by the buffer
    ByteList internal = new ByteList(
        utf8.array(), UTF8Encoding.INSTANCE, false /* don't copy */);
    // limit the ByteList to the buffer's current view
    internal.view(utf8.position(), utf8.remaining());
    // create a string backed by the ByteList
    return RubyString.newString(runtime, internal);
  }

  @Override
  public RubySymbol createSymbol(String symbol) {
    return RubySymbol.newSymbol(runtime, symbol);
  }

  @Override
  public RubyHash createMap() {
    return RubySymbolSupportHash.newSymbolSupportHash(runtime);
  }

  @Override
  public RubyArray createList() {
    return RubyArray.newArray(runtime);
  }

  @Override
  public RubyArray createList(int capacity) {
    return RubyArray.newArray(runtime, capacity);
  }

  private static RubyClass getStructClass(
      Ruby runtime, String name, List<String> fields) {
    RubyClass base = runtime.getStructClass();
    IRubyObject[] args = new IRubyObject[fields.size() + 1];
    args[0] = runtime.newString(name);
    for (int i = 1; i < args.length; i += 1) {
      args[i] = runtime.newSymbol(fields.get(i-1));
    }
    return RubyStruct.newInstance(base, args, Block.NULL_BLOCK);
  }
}
