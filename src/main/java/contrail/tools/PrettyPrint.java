/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * Pretty print avro files to json.
 *
 * TODO(jeremy@lewi.us):
 * 1. Allow the inputpath to specify a directory or glob path.
 * 2. If outputpath isn't specified dump to stdout.
 */
public class PrettyPrint extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(PrettyPrint.class);

  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  // TODO(jeremy@lewi.us): I was just lazy and declared this function
  // to throw an exception we should catch and handle the exceptions.
  public void prettyPrint() throws Exception {
    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }
    String inputFile = (String) this.stage_options.get("inputpath");
    String outputFile = (String) this.stage_options.get("outputpath");
    Path inPath = new Path(inputFile);
    Path outPath = new Path(outputFile);
    FSDataInputStream inStream = inPath.getFileSystem(getConf()).open(
        inPath);
    GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
    DataFileStream<Object> fileReader =
        new DataFileStream<Object>(inStream, reader);

    try {
      Schema schema = fileReader.getSchema();
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

      FSDataOutputStream outStream = outPath.getFileSystem(getConf()).create(
          outPath);
      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());

      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, generator);

      for (Object datum : fileReader) {
        writer.write(datum, encoder);
      }
      encoder.flush();

      outStream.flush();
      outStream.close();
    } finally {
      fileReader.close();
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath"};
    checkHasParametersOrDie(required_args);

    prettyPrint();
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PrettyPrint(), args);
    System.exit(res);
  }
}
