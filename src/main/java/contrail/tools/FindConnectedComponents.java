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

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import contrail.graph.ConnectedComponentData;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.IndexedGraph;

/**
 * This stage uses an indexed graph to split the graph into connected
 * components.
 */
public class FindConnectedComponents extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      FindConnectedComponents.class);
  // Set of the nodeids that have already been visited.
  private HashSet<String> visitedIds;

  // An iterator of all entries in the file.
  private Iterator<AvroKeyValue<CharSequence, GraphNodeData>> kvIterator;

  // The full graph.
  private IndexedGraph graph;

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(defs);
  }

  /**
   * Create an iterator over the nodes in the file.
   */
  protected void createNodeIterator() {
    String inputPath = (String)stage_options.get("inputpath");

    // TODO(jeremy@lewi.us): We should avoid hardcoding the name of the
    // data part of the sorted avro file. Is there a constant defined
    // in Avro for this?
    Path graphPath = new Path(FilenameUtils.concat(inputPath, "data"));
    DataFileStream<AvroKeyValue<CharSequence, GraphNodeData>> reader = null;
    try {
      FileSystem fs= graphPath.getFileSystem(getConf());
      FSDataInputStream inStream = fs.open(graphPath);
      SpecificDatumReader<AvroKeyValue<CharSequence, GraphNodeData>> dataReader =
          new SpecificDatumReader<AvroKeyValue<CharSequence, GraphNodeData>>();
       reader = new  DataFileStream<AvroKeyValue<CharSequence, GraphNodeData>>(
              inStream, dataReader);
    } catch (IOException e) {
      sLogger.fatal(
          "Couldn't open the input file:" + graphPath.toString(), e);
    }

    kvIterator = reader.iterator();
  }

  /**
   * Class used for topologically sorting a graph component
   */
  private static class NodeItem implements Comparable<NodeItem> {
    public GraphNode node;

    // Which strand of this node is used in the connected component.
    public DNAStrand strand;

    // Number of incoming edges to strand that haven't been visited yet.
    private int unvistedEdges;

    public NodeItem(GraphNode node, DNAStrand strand) {
      this.node = node;
      this.strand = strand;
      unvistedEdges = node.degree(strand, EdgeDirection.INCOMING);
    }

    public int compareTo(NodeItem other) {
      return node.degree(strand, EdgeDirection.INCOMING) -
             other.node.degree(other.strand, EdgeDirection.INCOMING);
    }

    public int getUnvistedEdges() {
      return unvistedEdges;
    }

    public void decrement() {
      --unvistedEdges;
      if (unvistedEdges < 0) {
        sLogger.fatal(
            "unvisted edges has become negative.",
            new RuntimeException("Topological sort error."));
      }
    }
  }

  protected ConnectedComponentData createComponent(
      HashMap<String, NodeItem> nodes) {
    // Create a priority heap based on the indegree of the forward strand.
    PriorityQueue<NodeItem> queue = new PriorityQueue<NodeItem>();
    queue.addAll(nodes.values());

    ArrayList<String> sorted = new ArrayList<String>(nodes.size());

    while (queue.size() > 0) {
      if (queue.peek().getUnvistedEdges() != 0) {
        // The graph must have a cycle so we can't sort it.
        break;
      }
      NodeItem current = queue.poll();
      sorted.add(current.node.getNodeId());

      // Decrement edges for all outgoing edges.
      for (EdgeTerminal next :
           current.node.getEdgeTerminals(
               current.strand, EdgeDirection.OUTGOING)) {
        nodes.get(next.nodeId).decrement();
      }
    }

    ArrayList<GraphNodeData> nodeData =
        new ArrayList<GraphNodeData>(nodes.size());


    ConnectedComponentData component = new ConnectedComponentData();

    if (queue.size() > 0) {
      // Graph has cycles so we can't sort it.
      component.setHasCycles(true);
      component.setSorted(false);

      for (NodeItem item : nodes.values()) {
        nodeData.add(item.node.getData());
      }
      component.setNodes(nodeData);
    } else {
      // Output the nodes in sorted order.
      component.setHasCycles(false);
      component.setSorted(true);
      for (String nodeId : sorted) {
        nodeData.add(nodes.get(nodeId).node.getData());
      }
      component.setNodes(nodeData);
    }

    return component;
  }

  protected HashMap<String, NodeItem> walkComponent(String startId) {
    HashMap<String, NodeItem> nodes = new HashMap<String, NodeItem>();
    ArrayList<EdgeTerminal> unprocessed = new ArrayList<EdgeTerminal>();

    unprocessed.add(new EdgeTerminal(startId, DNAStrand.FORWARD));

    boolean firstTerminal = true;

    GraphNode node;
    while (unprocessed.size() > 0){
      EdgeTerminal current = unprocessed.remove(unprocessed.size() - 1);
      node = new GraphNode(graph.lookupNode(current.nodeId));
      firstTerminal = false;

      NodeItem item = new NodeItem(node, current.strand);

      nodes.put(node.getNodeId(), item);

      visitedIds.add(node.getNodeId());

      ArrayList<EdgeTerminal> nextTerminals = new ArrayList<EdgeTerminal>();
      nextTerminals.addAll(
          node.getEdgeTerminals(current.strand, EdgeDirection.OUTGOING));
      nextTerminals.addAll(
          node.getEdgeTerminals(current.strand, EdgeDirection.INCOMING));

      for (EdgeTerminal next : nextTerminals) {
        if (visitedIds.contains(next.nodeId)) {
          continue;
        }
        unprocessed.add(next);
      }
    }

    return nodes;
  }

  protected void stageMain() {
    visitedIds = new HashSet<String>();

    createNodeIterator();
    graph = new IndexedGraph(
        (String)stage_options.get("inputpath"), getConf());

    // Writer for the connected components.
    Path outPath = new Path((String)stage_options.get("outputpath"));

    DataFileWriter<ConnectedComponentData> writer = null;
    try {
      FileSystem fs = outPath.getFileSystem(getConf());

      if (fs.exists(outPath) && fs.isDirectory(outPath)) {
        sLogger.fatal(
            "outputpath points to an existing directory but it should be a " +
            "file.");
      }
      FSDataOutputStream outStream = fs.create(outPath, true);
      Schema schema = (new ConnectedComponentData()).getSchema();
      DatumWriter<ConnectedComponentData> datumWriter =
          new SpecificDatumWriter<ConnectedComponentData>(schema);
      writer =
          new DataFileWriter<ConnectedComponentData>(datumWriter);
      writer.create(schema, outStream);

    } catch (IOException exception) {
      fail("There was a problem writing the components to an avro file. " +
           "Exception: " + exception.getMessage());
    }

    int numSorted = 0;
    int numUnsorted = 0;

    int dagNodes = 0;
    int nonDagNodes = 0;

    while (kvIterator.hasNext()) {
      // AvroKeyValue uses generic records so we just discard the
      // GraphNodeData and look it back up using IndexGraph to handle the
      // conversion to a specific record. This means we end up doing
      // two lookups for the first record which is inefficient.
      GenericRecord pair = (GenericRecord) kvIterator.next();
      String nodeId = pair.get("key").toString();
      if (visitedIds.contains(nodeId)) {
        continue;
      }

      sLogger.info("Walking component from node:" + nodeId);
      HashMap<String, NodeItem> subgraph = walkComponent(nodeId);

      // Output the connected component.
      sLogger.info("Sorting component from node:" + nodeId);
      ConnectedComponentData component = createComponent(subgraph);
      sLogger.info("Component size:" +  component.getNodes().size());

      if (component.getSorted()) {
        ++numSorted;
        dagNodes += component.getNodes().size();
      } else {
        ++numUnsorted;
        nonDagNodes += component.getNodes().size();
      }

      try {
        writer.append(component);
      } catch (IOException e) {
        sLogger.fatal("Couldn't write connected componet", e);
      }
    }

    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close:" + outPath.toString(), e);
    }

    sLogger.info(String.format(
        "Number sorted components:%d \t total nodes:%d", numSorted, dagNodes));
    sLogger.info(String.format(
        "Number components which aren't dags:%d \t total nodes:%d",
        numUnsorted, nonDagNodes));
  }

  public static void main(String[] args) throws Exception {
    FindConnectedComponents stage = new FindConnectedComponents();
    int res = stage.run(args);
    System.exit(res);
  }
}
