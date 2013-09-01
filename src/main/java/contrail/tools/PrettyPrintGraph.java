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
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import contrail.graph.GraphNodeData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * Pretty prints nodes in a file.
 * As an added convenience we decode the DNA sequence.
 */
public class PrettyPrintGraph extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(PrettyPrintGraph.class);

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
    Path inPath = new Path(inputFile);

    FSDataInputStream inStream = inPath.getFileSystem(getConf()).open(
        inPath);
    SpecificDatumReader<GraphNodeData> reader =
        new SpecificDatumReader<GraphNodeData>();
    DataFileStream<GraphNodeData> fileReader =
        new DataFileStream<GraphNodeData>(inStream, reader);

    // The output schema is a record containing the actual graph node and
    // the uncompressed sequence.
    ArrayList<Field> outFields = new ArrayList<Field>();
    outFields.add(new Field(
        "GraphNodeData", (new GraphNodeData()).getSchema(),
        "Actual node data.", null));
    outFields.add(new Field(
        "uncompressed_sequence", Schema.create(Schema.Type.STRING),
        "Uncompressed dna sequence..", null));

    Schema outSchema = Schema.createRecord(outFields);
    GenericData.Record outRecord = new GenericData.Record(outSchema);
    Sequence sequence = new Sequence(DNAAlphabetFactory.create());

    try {
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(outSchema);

      OutputStream outStream;
      if (outPath == null) {
        // Write to standard output.
        outStream = System.out;
      } else {
       outStream = outPath.getFileSystem(getConf()).create(outPath);
      }
      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());

      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          outSchema, generator);

      for (GraphNodeData datum : fileReader) {
        outRecord.put("GraphNodeData", datum);
        sequence.readCompressedSequence(datum.getSequence());

        outRecord.put("uncompressed_sequence", sequence.toString());
        writer.write(outRecord, encoder);
      }
      encoder.flush();

      outStream.flush();
      outStream.close();
    } finally {
      fileReader.close();
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PrettyPrintGraph(), args);
    System.exit(res);
  }

  @Override
  protected void stageMain() {
    try {
      prettyPrint();
    } catch (Exception e) {
      sLogger.fatal("There was a problem pretty printing the avro data.", e);
    }
  }
}
