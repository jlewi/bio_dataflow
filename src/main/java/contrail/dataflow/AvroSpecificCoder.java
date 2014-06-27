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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.services.dataflow.model.CloudNamedParameter;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.coders.StandardCoder;

import contrail.util.AvroSchemaUtil;

/**
 * An encoder for avro specific types.
 *
 * TODO(jlewi): We'd like to restrict T to be a subclass of
 * org.apache.avro.specific.SpecificRecordBase e.g.
 * AvroSpecificCoder<T> org.apache.avro.specific.SpecificRecordBase.
 *
 * However that creates a compile problem for of(String classType) because
 * Class.forName(classType)
 */
public class AvroSpecificCoder<T> extends StandardCoder<T> {

  /**
   * Returns an {@code AvroSpecificCoder} instance for the provided element type.
   * @param <T> the element type
   */
  public static <T> AvroSpecificCoder<T> of(Class<T> type) {
    return new AvroSpecificCoder<T>(type);
  }

  @JsonCreator
  public static <T> AvroSpecificCoder<T> of(@JsonProperty("type") String classType)
      throws ClassNotFoundException {
    return (AvroSpecificCoder<T>) of(Class.forName(classType));
  }

  private final Class<T> type;
  private final Schema schema;
  private final SpecificDatumWriter<T> writer;
  private final SpecificDatumReader<T> reader;
  private final EncoderFactory encoderFactory = new EncoderFactory();
  private final DecoderFactory decoderFactory = new DecoderFactory();

  protected AvroSpecificCoder(Class<T> type) {
    this.type = type;
    this.schema = AvroSchemaUtil.getSchemaForSpecificType(type);
    if (this.schema == null) {
      throw new RuntimeException("Could not get schema for class: " +
                                 type.getName());
    }
    this.writer = new SpecificDatumWriter<T>(schema);
    this.reader = new SpecificDatumReader<T>(schema);
  }

  public Class<T> getRecordType() {
    return type;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context)
      throws IOException {
    BinaryEncoder encoder = encoderFactory.binaryEncoder(outStream, null);
    writer.write(value, encoder);
    encoder.flush();

    // NO COMMIT
    ByteArrayOutputStream bStream = new ByteArrayOutputStream();
    BinaryEncoder debugEncoder = encoderFactory.binaryEncoder(bStream, null);
    writer.write(value, debugEncoder);
    debugEncoder.flush();
    System.out.println("Num bytes written:" + bStream.toByteArray().length);
  }

  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    BinaryDecoder decoder = decoderFactory.binaryDecoder(inStream, null);
    return reader.read(null, decoder);
  }

//  @Override
//  public CloudEncoding asCloudEncoding() {
//    // We convert this coder into CloudEncoding so that it can be serialized.
//    StandardCoder<Integer> t;
//    CloudEncoding encoding = new CloudEncoding();
//    encoding.setType(this.getClass().getName());
//
//    return encoding;
//  }

  @Override
  protected void addCloudEncodingDetails(
      Map<String, CloudNamedParameter> encodingParameters) {
    encodingParameters.put("type",
        new CloudNamedParameter().setStringValue(type.getName()));
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

  // Coder Factory allows us to register coders as defaults.
  public static class CoderFactory extends CoderRegistry.CoderFactory {
    private final Class type;

    public CoderFactory(Class type) {
      this.type = type;
    }
    @Override
    public Coder create(List<? extends Coder> typeArgumentCoders) {
      return AvroSpecificCoder.of(type);
    }
  }
}

