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

package com.cloudera.crash.generics.python;

import com.cloudera.crash.generics.CustomData;
import com.cloudera.crash.generics.DataFactory;
import com.cloudera.crash.generics.ruby.RubyDataFactory;
import org.jruby.Ruby;
import org.jruby.RubyInteger;
import org.jruby.RubyNumeric;
import org.jruby.RubyStruct;
import org.jruby.RubySymbol;
import org.jruby.javasupport.JavaUtil;
import org.python.core.Py;
import org.python.core.PyBaseString;
import org.python.core.PyDictionary;
import org.python.core.PyLong;
import org.python.core.PyNone;
import org.python.core.PyObject;
import org.python.core.PyString;
import org.python.core.PyStringMap;
import org.python.modules.struct;

/**
 * Ruby-specific alterations to CustomData.
 */
public class PyData extends CustomData {

  private static final PyData INSTANCE = new PyData();

  public static PyData get() {
    return INSTANCE;
  }

  private PyData() {
    super(PyDataFactory.get(), true /* reuse containers */ );
  }

  @Override
  protected boolean isRecord(Object datum) {
    return (datum instanceof PyStringMap);
  }

  @Override
  protected boolean isEnum(Object datum) {
    // symbols are part of an enum if they have an known schema
    return (datum instanceof PyString) && hasSchema(((PyString) datum).internedString());
  }

  @Override
  protected boolean isString(Object datum) {
    // use PyBaseString, which both PyUnicode and PyString extend
    return (datum instanceof CharSequence) || (datum instanceof PyBaseString);
  }

  @Override
  protected boolean isNull(Object datum) {
    return (datum instanceof PyNone);
  }

  @Override
  public void setField(Object record, String name, int pos, Object value) {
    PyStringMap strmap = (PyStringMap) record; // problems => ClassCastException
    // strmap uses interned strings
    strmap.__setitem__(name.intern(), Py.java2py(value));
  }

  @Override
  public Object getField(Object record, String name, int position) {
    PyStringMap strmap = (PyStringMap) record; // problems => ClassCastException
    return Py.tojava(strmap.__getitem__(name.intern()), Object.class);
  }

  private Object pyToJava(PyObject obj) {
    return Py.tojava(obj, Object.class);
  }
}
