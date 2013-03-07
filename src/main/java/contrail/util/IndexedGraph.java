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
package contrail.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import contrail.graph.GraphNodeData;

/**
 * Provides random access to a graph stored in a sorted/indexed avro file.
 */
public class IndexedGraph {
  // TODO(jeremy@lewi.us): Should we implement a cache to cache recently looked
  // up nodes? If we do we should probably clone the node before returning it.
  private static final Logger sLogger = Logger.getLogger(IndexedGraph.class);
  private SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader;

  /**
   * Create the indexed graph based on the inputpath.
   * @param inputPath: Path to the graph.
   */
  public IndexedGraph(String inputPath, Configuration conf) {
    createReader(inputPath, conf);
  }

  private void createReader(String inputPath, Configuration conf) {
    SortedKeyValueFile.Reader.Options readerOptions =
        new SortedKeyValueFile.Reader.Options();

    readerOptions.withPath(new Path(inputPath));

    GraphNodeData nodeData = new GraphNodeData();
    readerOptions.withConfiguration(conf);
    readerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    readerOptions.withValueSchema(nodeData.getSchema());

    reader = null;
    try {
      reader = new SortedKeyValueFile.Reader<CharSequence,GraphNodeData> (
          readerOptions);
    } catch (IOException e) {
      sLogger.fatal("Couldn't open indexed avro file.", e);
    }
  }

  /**
   * Read the node from the sorted key value file.
   * @param reader
   * @param nodeId
   * @return
   */
  public GraphNodeData lookupNode(String nodeId) {
    GenericRecord record = null;
    GraphNodeData nodeData = new GraphNodeData();
    try{
      // The actual type returned by get is a generic record even
      // though the return type is GraphNodeData. I have no idea
      // how SortedKeyValueFileReader actually compiles.
      record =  (GenericRecord) reader.get(nodeId);
    } catch (IOException e) {
      sLogger.fatal("There was a problem reading from the file.", e);
      System.exit(-1);
    }
    if (record == null) {
      sLogger.fatal(
          "Could not find node:" + nodeId,
          new RuntimeException("Couldn't find node"));
      System.exit(-1);
    }

    // Convert the Generic record to a specific record.
    try {
      // TODO(jeremy@lewi.us): We could probably make this code
      // more efficient by reusing objects.
      GenericDatumWriter<GenericRecord> datumWriter =
          new GenericDatumWriter<GenericRecord>(record.getSchema());

      ByteArrayOutputStream outStream = new ByteArrayOutputStream();
      BinaryEncoder encoder =
          EncoderFactory.get().binaryEncoder(outStream, null);

      datumWriter.write(record, encoder);
      // We need to flush the encoder to write the data to the byte
      // buffer.
      encoder.flush();
      outStream.flush();

      // Now read it back in as a specific datum reader.
      ByteArrayInputStream inStream = new ByteArrayInputStream(
          outStream.toByteArray());

      BinaryDecoder decoder =
          DecoderFactory.get().binaryDecoder(inStream, null);
      SpecificDatumReader<GraphNodeData> specificReader = new
          SpecificDatumReader<GraphNodeData>(nodeData.getSchema());

      specificReader.read(nodeData, decoder);
    } catch (IOException e) {
      sLogger.fatal(
          "There was a problem converting the GenericRecord to " +
          "GraphNodeData", e);
      System.exit(-1);
    }
    return nodeData;
  }
}
