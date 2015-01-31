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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This class is used to walk a graph starting at some seed nodes
 * and walking outwards. All nodes visited are then outputted.
 * The input files must be SortedKeyValueFile's so that we can efficiently
 * lookup nodes in the files.
 *
 * TODO(jeremy@lewi.us): We should probably use the GraphBFSIterator
 * in this code.
 */
public class WalkGraph extends NonMRStage {
  // TODO(jlewi): Use Indexed graph rather than implementing the methods
  // lookup and createreader.
  private static final Logger sLogger =
      Logger.getLogger(WalkGraph.class);

  /**
   * Keep track of all nodes already written so we only write each node once.
   */
  private final HashSet<String> outputted;

  public WalkGraph() {
    outputted = new HashSet<String>();
  }

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition startNodes = new ParameterDefinition(
        "start_nodes", "Comma separated list of the nodes to start the walk " +
        "from", String.class, null);

    defs.put(startNodes.getName(), startNodes);

    ParameterDefinition numHops = new ParameterDefinition(
        "num_hops", "Number of hops to take starting at start_nodes.",
        Integer.class, null);

    ParameterDefinition makeValid = new ParameterDefinition(
        "make_valid", "Set this to true to prune edges so that the " +
        "resulting graph is a valid graph.",
        Boolean.class, false);

    defs.put(numHops.getName(), numHops);
    defs.put(makeValid.getName(), makeValid);

    return Collections.unmodifiableMap(defs);
  }

  private SortedKeyValueFile.Reader<CharSequence, GraphNodeData> createReader()
      {
    SortedKeyValueFile.Reader.Options readerOptions =
        new SortedKeyValueFile.Reader.Options();

    String inputPath = (String) stage_options.get("inputpath");
    readerOptions.withPath(new Path(inputPath));

    GraphNodeData nodeData = new GraphNodeData();
    readerOptions.withConfiguration(getConf());
    readerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    readerOptions.withValueSchema(nodeData.getSchema());

    SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader = null;
    try {
      reader = new SortedKeyValueFile.Reader<CharSequence,GraphNodeData> (
          readerOptions);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return reader;
  }

  /**
   * Read the node from the sorted key value file.
   * @param reader
   * @param nodeId
   * @return
   */
  private GraphNodeData lookupNode(
      SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader,
      String nodeId) {
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

  /**
   * Walk the graph from the start node.
   * @param startId
   * @param numHops
   * @param writer
   * @param exclude: List of nodes already outputted so we exclude them.
   * @return: List of all nodes visited.
   */
  private HashSet<String> walk(
      SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader,
      String[] startIds, int numHops,
      DataFileWriter<GraphNodeData> writer) {
    HashSet<String> visited = new HashSet<String>();

    // Use two lists so we can keep track of the hops.
    HashSet<String> thisHop = new HashSet<String>();
    HashSet<String> nextHop = new HashSet<String>();

    boolean makeValid = (Boolean) stage_options.get("make_valid");
    int hop = 0;
    thisHop.addAll(Arrays.asList(startIds));
    GraphNodeData nodeData = new GraphNodeData();
    GraphNode node = new GraphNode();

    while (hop <= numHops && thisHop.size() > 0) {
      // TODO(jlewi): We should really walk from all the start nodes
      // at once to make sure we prune the graph correctly.
      if (hop == numHops && makeValid) {
        // We need to prune any edges that won't be included in the graph.
        int numPruned = 0;
        for (String nodeId : thisHop) {
          nodeData = lookupNode(reader, nodeId);
          node.setData(nodeData);
          for (String neighborId : node.getNeighborIds()) {
            if (!visited.contains(neighborId) &&
                !thisHop.contains(neighborId)) {
              node.removeNeighbor(neighborId);
              ++numPruned;
            }
          }
          try {
            if (!outputted.contains(nodeId)) {
              writer.append(nodeData);
              outputted.add(nodeId);
            }
          } catch (IOException e) {
            sLogger.fatal("There was a problem writing the node", e);
            System.exit(-1);
          }
          visited.add(nodeId);
        }
        sLogger.info(String.format(
            "Deleted %d edges to make the graph valid.", numPruned));
      } else {
        // Fetch each node in thisHop.
        for (String nodeId : thisHop) {
          if (!visited.contains(nodeId)) {
            nodeData = lookupNode(reader, nodeId);
            try{
              if (!outputted.contains(nodeId)) {
                writer.append(nodeData);
                outputted.add(nodeId);
              }
            } catch (IOException e) {
              sLogger.fatal("There was a problem writing the node", e);
              System.exit(-1);
            }
            visited.add(nodeId);
            node.setData(nodeData);
            nextHop.addAll(node.getNeighborIds());
          }
        }
      }
      thisHop.clear();
      thisHop.addAll(nextHop);
      nextHop.clear();
      ++hop;
    }
    return visited;
  }

  /**
   * Find the subgraph by starting at the indicated node and walking the
   * specified number of hops.
   */
  private void writeSubGraph() {
    String outputPath = (String) stage_options.get("outputpath");
    String startNodes = (String) stage_options.get("start_nodes");
    int numHops = (Integer) stage_options.get("num_hops");

    String[] nodeids = startNodes.split(",");

    SortedKeyValueFile.Reader<CharSequence, GraphNodeData> reader =
        createReader();

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }

    GraphNodeData node = new GraphNodeData();

    // TODO(jeremy@lewi.us): Output path must exist.
    try {
      if (!fs.exists(new Path(outputPath))) {
        sLogger.info("Creating output path:" + outputPath);
        fs.mkdirs(new Path(outputPath));
      }
    } catch (IOException e) {
      sLogger.fatal("Could not create the outputpath:" + outputPath, e);
      System.exit(-1);
    }

    String outputFile = FilenameUtils.concat(outputPath, "subgraph.avro");
    FSDataOutputStream outStream = null;
    DataFileWriter<GraphNodeData> avroStream = null;
    SpecificDatumWriter<GraphNodeData> writer = null;
    try {
      outStream = fs.create(new Path(outputFile));
      writer =
          new SpecificDatumWriter<GraphNodeData>(GraphNodeData.class);
      avroStream =
          new DataFileWriter<GraphNodeData>(writer);
      avroStream.create(node.getSchema(), outStream);
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }

    walk(reader, nodeids, numHops, avroStream);

    try {
      avroStream.close();
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close the output stream.", e);
      System.exit(-1);
    }
  }

  @Override
  protected void stageMain() {
    writeSubGraph();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WalkGraph(), args);
    System.exit(res);
  }
}
