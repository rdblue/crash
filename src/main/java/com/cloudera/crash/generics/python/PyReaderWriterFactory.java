package com.cloudera.crash.generics.python;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.crunch.types.avro.ReaderWriterFactory;

public class PyReaderWriterFactory implements ReaderWriterFactory {
  @Override
  public GenericData getData() {
    return PyData.get();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <D> DatumReader<D> getReader(Schema schema) {
    return PyData.get().createDatumReader(schema);
  }

  @Override
  public <D> DatumWriter<D> getWriter(Schema schema) {
    return PyData.get().createDatumWriter(schema);
  }
}
