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
package contrail.dataflow.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.dataflow.AvroSpecificCoder;
import contrail.dataflow.GCSAvroFileSplit;
import contrail.dataflow.ReadAvroSpecificDoFn;
import contrail.util.AvroSchemaUtil;

/**
 * A transform for encoding Avro records as json with no newlines.
 *
 * This is intended as a way to output Avro r ecds
 */
public class EncodeAvroAsJson<AvroType>
    extends PTransform<PCollection<AvroType>, PCollection<String>> {

  // Note. We can mark the schema as transient because we just need to
  // pass it along to the DoFn.
  private final transient Schema schema;

  private class EncodeAsJson extends DoFn<AvroType, String> {
    // Schema isn't serializable so we use the string representation
    // of the schema for serialization.
    private transient Schema schema;
    private final String jsonSchema;
    /**
     * @param schema The schema of the avro records.
     */
    public EncodeAsJson(Schema schema) {
      this.schema = schema;
      this.jsonSchema = schema.toString();
    }

    @Override
    public void startBatch(Context c) {
      // Parse the schema;
      Schema.Parser parser = new Schema.Parser();
      schema = parser.parse(jsonSchema);
    }

    @Override
    public void processElement(ProcessContext c) {
      AvroType avroRecord = c.element();

      DatumWriter<AvroType> writer = new SpecificDatumWriter<AvroType>(schema);

      JsonFactory factory = new JsonFactory();
      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      try {
        JsonGenerator generator = factory.createJsonGenerator(outStream);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, generator);

        writer.write(avroRecord, encoder);
        encoder.flush();

        outStream.flush();
        outStream.close();
      } catch (IOException e) {
        // TODO(jlewi): Add a counter to keep track of the number of records
        // that couldn't be encoded and were dropped.
        return;
      }
      c.output(outStream.toString());
    }
  }

  /**
   * @param schema The schema of the avro records.
   */
  public EncodeAvroAsJson(Schema schema) {
    this.schema = schema;
  }

  @Override
  public PCollection<String> apply(PCollection<AvroType> input) {
    return input.apply(ParDo.of(new EncodeAsJson(schema)));
  }
}
