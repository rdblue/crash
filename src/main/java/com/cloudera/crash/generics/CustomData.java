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


import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class CustomData extends GenericData {

  public static Schema schemaFromResource(String resource) {
    try {
      return new Schema.Parser().parse(
          Resources.getResource(resource).openStream());
    } catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }
  public static final Schema GENERIC_SCHEMA = schemaFromResource("generic.avsc");

  protected static final String CONTAINER = "Container";

  private final LoadingCache<Schema, List<String>> fieldNamesCache =
      CacheBuilder.newBuilder().build(
          CacheLoader.from(new Function<Schema, List<String>>() {
            @Override
            public List<String> apply(@Nullable Schema schema) {
              if (schema != null && schema.getType() == Schema.Type.RECORD) {
                List<Schema.Field> fields = schema.getFields();
                List<String> names = Lists.newArrayListWithCapacity(fields.size());
                for (Schema.Field field : fields) {
                  names.add(field.name());
                }
                return names;
              } else {
                return null;
              }
            }
          }));

  /*
   * Keep a store of the schemas for objects that have been created by this
   * CustomData implementation. The keys are weak references so they do not
   * keep the created data objects from being garbage collected.
   */
  private final Cache<Object, Schema> schemaCache =
      CacheBuilder.newBuilder().weakKeys().build();

  /*
   * record instanceof IndexedRecord
   * array instanceof Collection
   * map instanceof Map
   * fixed instanceof GenericFixed
   * string instanceof CharSequence
   * bytes instanceof ByteBuffer
   * int instanceof Integer
   * long instanceof Long
   * float instanceof Float
   * double instanceof Double
   * bool instanceof Boolean
   * symbol instanceof GenericEnumSymbol, could be replaced if passed Class<T>
   */

  private final DataFactory factory;
  private boolean reuseContainers = true;

  protected CustomData(DataFactory factory, boolean shouldReuseContainers) {
    this.factory = factory;
    this.reuseContainers = shouldReuseContainers;
  }

  @Override
  public DatumReader createDatumReader(Schema schema) {
    return new CustomDatumReader(schema, this);
  }

  public <D> DatumWriter<D> createDatumWriter(Schema schema) {
    return new CustomDatumWriter<D>(schema, this);
  }

  public void reuseContainers(boolean shouldReuse) {
    this.reuseContainers = shouldReuse;
  }

  public boolean shouldReuseContainers() {
    return this.reuseContainers;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object newRecord(Object old, Schema schema) {
    if (shouldReuseContainers() && old != null) {
      // GenericData checks schemas via object equality, so do it here
      if (isRecord(old) && schema == getRecordSchema(old)) {
        // no need to clear the old record; all fields will be replaced
        return old;
      }
    }

    final Object record;
    try {
      record = factory.createRecord(schema.getName(), fieldNamesCache.get(schema));
    } catch (ExecutionException ex) {
      throw Throwables.propagate(ex);
    }
    schemaCache.put(record, schema);
    return record;
  }

  @Override
  protected Schema getRecordSchema(Object record) {
    return schemaCache.getIfPresent(record);
  }

  @Override
  protected Schema getEnumSchema(Object enu) {
    return schemaCache.getIfPresent(enu);
  }

  protected void addEnumSchema(Schema schema) {
    for (String enu : schema.getEnumSymbols()) {
      schemaCache.put(factory.createSymbol(enu), schema);
    }
  }

  protected boolean hasSchema(Object datum) {
    return (schemaCache.getIfPresent(datum) != null);
  }

  protected boolean isNull(Object datum) {
    // no need to check if really null, that is done in super.getSchemaName
    return false;
  }

  @Override
  protected String getSchemaName(Object datum) {
    if (isNull(datum)) {
      return Schema.Type.NULL.getName();
    } else {
      return super.getSchemaName(datum);
    }
  }

  @Override
  public <T> T deepCopy(Schema schema, T value) {
    // TODO: eventually replace this with something that hooks object creation
    return super.deepCopy(schema, value);
  }

  public Object newString(String value) {
    return factory.createString(value);
  }

  public Object newString(ByteBuffer utf8) {
    return factory.createString(utf8);
  }

  public Object newMap(Object old, int sizeHint) {
    if (shouldReuseContainers() && old != null) {
      if (isMap(old)) {
        ((Map) old).clear();
        return old;
      }
    }
    return factory.createMap();
  }

  public Object newSymbol(String symbol, Schema schema) {
    Object sym = factory.createSymbol(symbol);
    // make sure the schema is cached for all of the symbols in this enum
    if (schema != schemaCache.getIfPresent(sym)) {
      addEnumSchema(schema);
    }
    return sym;
  }

  public Object newList(Object old, int size, Schema schema) {
    if (shouldReuseContainers() && old != null) {
      // TODO: add support for GenericArray to reuse contained objects
      if (isArray(old) && schema == schemaCache.getIfPresent(old)) {
        ((Collection) old).clear();
        return old;
      }
    }
    Object list = factory.createList(size);
    schemaCache.put(list, schema);
    return list;
  }

  public CharSequence makeString(Object datum) {
    // By default, coerce non-CharSequence objects with toString()
    return datum.toString();
  }
}
