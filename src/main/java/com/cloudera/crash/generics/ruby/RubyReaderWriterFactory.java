package com.cloudera.crash.generics.ruby;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.crunch.types.avro.ReaderWriterFactory;

public class RubyReaderWriterFactory implements ReaderWriterFactory {
  @Override
  public GenericData getData() {
    return RubyData.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D> DatumReader<D> getReader(Schema schema) {
    return RubyData.get().createDatumReader(schema);
  }

  @Override
  public <D> DatumWriter<D> getWriter(Schema schema) {
    return RubyData.get().createDatumWriter(schema);
  }
}
