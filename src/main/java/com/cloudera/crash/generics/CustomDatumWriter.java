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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import java.io.IOException;

public class CustomDatumWriter<D> extends GenericDatumWriter<D> {
  private final CustomData data;

  public CustomDatumWriter(Schema schema, CustomData data) {
    super(schema, data);
    System.err.println("Instantiated writer for " + schema.toString(true));
    this.data = data;
  }

  @Override
  public CustomData getData() {
    return data;
  }

  @Override
  protected void writeRecord(Schema schema, Object datum, Encoder out) throws IOException {
    /*
     * An alternative to skipping the record entirely is to still write it out
     * by detecting the situation in getRecordState and using the state object
     * to signal that getField should return the content field for an imaginary
     * record. This way, the file will still be readable by other Avro libs.
     */
    if (CustomData.CONTAINER.equals(schema.getName())) {
      // skip writing this record and write its single field instead
      write(schema.getFields().get(0).schema(), datum, out);
    } else {
      super.writeRecord(schema, datum, out);
    }
  }

  @Override
  public void writeString(Object datum, Encoder out) throws IOException {
    /*
     * Objects that are written as strings may not implement CharSequence.
     * Delegate to the data implementation to convert it to a String
     */
    if (datum instanceof CharSequence) {
      super.writeString(datum, out);
    } else {
      super.writeString(data.makeString(datum), out);
    }
  }
}
