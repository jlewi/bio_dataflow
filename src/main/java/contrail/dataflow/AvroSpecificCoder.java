/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package contrail.dataflow;

import com.google.api.services.dataflow.model.CloudNamedParameter;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * An encoder for avro specific types.
 * 
 */
public class AvroSpecificCoder<T extends org.apache.avro.specific.SpecificRecordBase> extends StandardCoder<T> {

  /**
   * Returns an {@code AvroCoder} instance for the provided element type.
   * @param <T> the element type
   */
  public static <T extends org.apache.avro.specific.SpecificRecordBase> AvroSpecificCoder<T> of(Schema schema) {
    return new AvroSpecificCoder<T>(schema);
  }
  
  private Schema schema;
  private SpecificDatumWriter<T> writer;
  private SpecificDatumReader<T> reader;
  private final EncoderFactory encoderFactory = new EncoderFactory();
  private final DecoderFactory decoderFactory = new DecoderFactory();

  public AvroSpecificCoder(Schema schema) {
    this.writer = new SpecificDatumWriter<T>(schema);
    this.reader = new SpecificDatumReader<T>(schema);
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws IOException {
    BinaryEncoder encoder = encoderFactory.binaryEncoder(outStream, null);
    writer.write(value, encoder);
    encoder.flush();
  }

  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    BinaryDecoder decoder = decoderFactory.binaryDecoder(inStream, null);
    return reader.read(null, decoder);
  }

  @Override
  public List<? extends Coder> getCoderArguments() {
    return null;
  }

  @Override
  public boolean isDeterministic() {
    // Avro serialization should be deterministic for a given schema.
    return true;
  }
}

