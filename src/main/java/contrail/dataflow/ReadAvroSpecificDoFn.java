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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsFilename;

import contrail.util.AvroSchemaUtil;

public class ReadAvroSpecificDoFn<AvroType>
    extends DoFn<GCSAvroFileSplit, AvroType> {
  private static final Logger sLogger = Logger.getLogger(
      ReadAvroSpecificDoFn.class);
  private final Class specificTypeClass;

  public ReadAvroSpecificDoFn(Class specificTypeClass) {
    this.specificTypeClass = specificTypeClass;
  }

  @Override
  public void processElement(ProcessContext c) {
    GCSAvroFileSplit split = c.element();
    System.out.println(split.getPath());

    GcsFilename gcsFilename = GcsUtil.asGcsFilename(split.getPath().toString());

    ByteChannel inChannel;
    try {
      GcsUtil gcsUtil = GcsUtil.create(c.getPipelineOptions());
      inChannel = gcsUtil.open(gcsFilename);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
      return;
    }
    InputStream inStream = Channels.newInputStream(inChannel);
    Schema schema = AvroSchemaUtil.getSchemaForSpecificType(
        specificTypeClass);
    SpecificDatumReader<AvroType> datumReader =
        new SpecificDatumReader<AvroType>(schema);

    DataFileStream<AvroType> fileReader;
    try {
      fileReader = new DataFileStream<AvroType>(inStream, datumReader);
    } catch(IOException e) {
      sLogger.error("Could not read file:" + split.getPath().toString());
      return;
    }

    while(fileReader.hasNext()) {
      AvroType datum = fileReader.next();
      c.output(datum);
    }
    try {
      fileReader.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
