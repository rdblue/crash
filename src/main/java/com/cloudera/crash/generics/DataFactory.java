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

package com.cloudera.crash.generics;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.generic.GenericRecord;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public interface DataFactory<R, M, L, S, E> {

  public R createRecord(String name, List<String> fieldNames);

  public M createMap();

  public L createList();

  public L createList(int capacity);

  public S createString(String javaString);

  public S createString(ByteBuffer utf8);

  public E createSymbol(String symbol);

  public static class Default implements DataFactory<GenericRecord, Map, List, String, String> {
    @Override
    public GenericRecord createRecord(String name, List<String> fieldNames) {
      return null;
    }

    @Override
    public String createString(String javaString) {
      return javaString;
    }

    @Override
    public String createString(ByteBuffer utf8) {
      // TODO: make this not suck
      return Charset.forName("utf-8").decode(utf8).toString();
    }

    @Override
    public String createSymbol(String symbol) {
      return symbol;
    }

    @Override
    public Map createMap() {
      return Maps.newHashMap();
    }

    @Override
    public List createList() {
      return Lists.newArrayList();
    }

    @Override
    public List createList(int capacity) {
      return Lists.newArrayListWithCapacity(capacity);
    }
  }
}
