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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.nio.ByteBuffer;

public class CustomDatumReader<D> extends GenericDatumReader<D> {

  private final CustomData data;

  protected CustomDatumReader(CustomData data) {
    this(null, data);
  }

  protected CustomDatumReader(Schema schema, CustomData data) {
    this(schema, schema, data);
  }

  protected CustomDatumReader(Schema writer, Schema reader, CustomData data) {
    super(writer, reader, data);
    System.err.println("Instantiated reader for " + (reader != null ? reader.toString(true) : "(null)"));
    this.data = data;
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    if (CustomData.CONTAINER.equals(expected.getName())) {
      // skip the generic Container record
      return read(old, expected.getFields().get(0).schema(), in);
    } else {
      return super.readRecord(old, expected, in);
    }
  }

  @Override
  public CustomData getData() {
    return data;
  }

  @Override
  protected Object createEnum(String symbol, Schema schema) {
    return data.newSymbol(symbol, schema);
  }

  @Override
  protected Object createString(String value) {
    return data.newString(value);
  }

  @Override
  protected Object readString(Object old, Schema expected, Decoder in) throws IOException {
    // TODO: track the backing ByteBuffer and reuse
    Utf8 scratch = in.readString(null);
    return data.newString(ByteBuffer.wrap(scratch.getBytes()));
  }

  @Override
  protected Object newMap(Object old, int size) {
    return data.newMap(old, size);
  }

  @Override
  protected Object newArray(Object old, int size, Schema schema) {
    return data.newList(old, size, schema);
  }

}
