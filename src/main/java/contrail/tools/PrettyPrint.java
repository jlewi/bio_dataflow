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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.FileHelper;

/**
 * Pretty print avro files to json.
 *
 * TODO(jeremy@lewi.us):
 * 1. Allow the inputpath to specify a directory or glob path.
 */
public class PrettyPrint extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(PrettyPrint.class);

  @Override
  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    defs.remove("outputpath");
    // Make outputpath an optional argument. Default will be to use standard
    // output.
    ParameterDefinition output = new ParameterDefinition(
        "outputpath", "The file to write the data to. Default is stdout.",
        String.class, "");
    defs.put(output.getName(), output);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Read the specified file and pretty print it to outstream.
   * @param inputFile
   * @param outstream
   */
  private void readFile(Path inPath, OutputStream outStream) {
    try {
      FSDataInputStream inStream = inPath.getFileSystem(getConf()).open(
          inPath);
      GenericDatumReader<Object> reader = new GenericDatumReader<Object>();
      DataFileStream<Object> fileReader =
          new DataFileStream<Object>(inStream, reader);

      Schema schema = fileReader.getSchema();
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());

      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, generator);

      for (Object datum : fileReader) {
        writer.write(datum, encoder);
      }
      encoder.flush();

      outStream.flush();
      fileReader.close();
    } catch(IOException e){
      sLogger.fatal("IOException.", e);
    }
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

    Path outPath = null;
    if (!outputFile.isEmpty()) {
      outPath = new Path(outputFile);
    }

    ArrayList<Path> matchingFiles = FileHelper.matchGlobWithDefault(
        getConf(), inputFile, "*.avro");
    try {
      OutputStream outStream = null;
      if (outPath == null) {
        // Write to standard output.
        outStream = System.out;
      } else {
        outStream = outPath.getFileSystem(getConf()).create(outPath);
      }

      for (Path inFile : matchingFiles) {
        readFile(inFile, outStream);
      }
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("IOException", e);
    }
  }

  @Override
  protected void stageMain() {
    try {
      prettyPrint();
    } catch (Exception e) {
      sLogger.fatal("There was a problem pretty printing the avro data.", e);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PrettyPrint(), args);
    System.exit(res);
  }
}
