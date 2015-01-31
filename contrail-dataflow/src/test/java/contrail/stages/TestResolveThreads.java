package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAStrand;
import contrail.util.FileHelper;

public class TestResolveThreads extends ResolveThreads {
  private static class TestCase {
    public HashMap<String, GraphNode> graph;
    public HashMap<String, GraphNode> expected;
    // The id of the node to resolve.
    public String nodeId;
    public TestCase() {
      graph = new HashMap<String, GraphNode>();
      expected = new HashMap<String, GraphNode>();
    }
  }

  @Test
  public void testResolveMultiplePairs() {
    // Test the case where a incoming edge to a node is paired with multiple
    // output edges.
    GraphNode node = GraphTestUtil.createNode("node", "AAA");
    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    int maxThreads = 100;

    List<String> tags1 = Arrays.asList("read1");
    List<String> tags2 = Arrays.asList("read2");
    List<String> allTags = new ArrayList<String>();
    allTags.addAll(tags1);
    allTags.addAll(tags2);

    // Add edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        allTags, maxThreads);

    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
        tags1, maxThreads);

    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out2", DNAStrand.FORWARD),
        tags2, maxThreads);

    nodes.put(node.getNodeId(), node.clone());
    {
      GraphNode inNode = GraphTestUtil.createNode("in", "AAA");
      inNode.addOutgoingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      nodes.put(inNode.getNodeId(), inNode);
    }

    {
      GraphNode outNode = GraphTestUtil.createNode("out1", "AAA");
      outNode.addIncomingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      nodes.put(outNode.getNodeId(), outNode);
    }

    {
      GraphNode outNode = GraphTestUtil.createNode("out2", "AAA");
      outNode.addIncomingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      nodes.put(outNode.getNodeId(), outNode);
    }

    resolveSpanningReadPaths(nodes, node.getNodeId());

    assertEquals(5, nodes.size());

    {
      // The first cloned node should have the edges to in and out1.
      GraphNode expectedClone = GraphTestUtil.createNode("node.01", "AAA");
      expectedClone.setCoverage(tags1.size());
      expectedClone.addIncomingEdgeWithTags(
          DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
          tags1, maxThreads);
      expectedClone.addOutgoingEdgeWithTags(
          DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
          tags1, maxThreads);

      GraphNode actualClone = nodes.get(expectedClone.getNodeId());
      assertEquals(expectedClone, actualClone);
    }

    {
      // The second cloned node should have the edges to in and out2.
      GraphNode expectedClone = GraphTestUtil.createNode("node.02", "AAA");
      expectedClone.setCoverage(tags1.size());
      expectedClone.addIncomingEdgeWithTags(
          DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
          tags2, maxThreads);
      expectedClone.addOutgoingEdgeWithTags(
          DNAStrand.FORWARD, new EdgeTerminal("out2", DNAStrand.FORWARD),
          tags2, maxThreads);

      GraphNode actualClone = nodes.get(expectedClone.getNodeId());
      assertEquals(
          GraphNode.NodeDiff.NONE, expectedClone.equalsWithInfo(actualClone));
    }

    {
      // The incoming node has edges to both clones.
      GraphNode expectedIn = GraphTestUtil.createNode("in", "AAA");
      expectedIn.addOutgoingEdgeWithTags(
          DNAStrand.FORWARD,
          new EdgeTerminal("node.01", DNAStrand.FORWARD), tags1, maxThreads);
      expectedIn.addOutgoingEdgeWithTags(
          DNAStrand.FORWARD,
          new EdgeTerminal("node.02", DNAStrand.FORWARD), tags2, maxThreads);
      GraphNode actualIn = nodes.get(expectedIn.getNodeId());
      assertEquals(GraphNode.NodeDiff.NONE, expectedIn.equalsWithInfo(actualIn));
    }

    {
      GraphNode expectedOut= GraphTestUtil.createNode("out1", "AAA");
      expectedOut.addIncomingEdgeWithTags(
          DNAStrand.FORWARD,
          new EdgeTerminal("node.01", DNAStrand.FORWARD), tags1, maxThreads);

      assertEquals(expectedOut, nodes.get(expectedOut.getNodeId()));
    }

    {
      GraphNode expectedOut= GraphTestUtil.createNode("out2", "AAA");
      expectedOut.addIncomingEdgeWithTags(
          DNAStrand.FORWARD,
          new EdgeTerminal("node.02", DNAStrand.FORWARD), tags2, maxThreads);

      GraphNode actualOut = nodes.get(expectedOut.getNodeId());
      assertEquals(
          GraphNode.NodeDiff.NONE,
          expectedOut.equalsWithInfo(actualOut));
    }
  }

  private TestCase createBasic() {
    TestCase testCase = new TestCase();

    GraphNode node = GraphTestUtil.createNode("node", "AAA");
    testCase.nodeId = node.getNodeId();
    int maxThreads = 100;

    List<String> tags1 = Arrays.asList("read1");
    List<String> tags2 = Arrays.asList("read2");
    // Add two incoming edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        tags1, maxThreads);
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in2", DNAStrand.FORWARD),
        tags2, maxThreads);

    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
        tags1, maxThreads);

    testCase.graph.put(node.getNodeId(), node.clone());
    {
      GraphNode inNode = GraphTestUtil.createNode("in", "AAA");
      inNode.addOutgoingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      testCase.graph.put(inNode.getNodeId(), inNode);
    }

    {
      GraphNode inNode2 = GraphTestUtil.createNode("in2", "AAA");
      inNode2.addOutgoingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      testCase.graph.put(inNode2.getNodeId(), inNode2);
    }

    {
      GraphNode outNode = GraphTestUtil.createNode("out1", "AAA");
      outNode.addIncomingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD));
      testCase.graph.put(outNode.getNodeId(), outNode);
    }

    {
      // The cloned node should have the edges to in and out1.
      GraphNode expectedClone = GraphTestUtil.createNode("node.01", "AAA");
      expectedClone.setCoverage(tags1.size());
      expectedClone.addIncomingEdgeWithTags(
          DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
          tags1, maxThreads);
      expectedClone.addOutgoingEdgeWithTags(
          DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
          tags1, maxThreads);

      testCase.expected.put(expectedClone.getNodeId(), expectedClone);
    }

    {
      // The original node should remain connected to the node which wasn't
      // connected to any outputs.
      GraphNode expectedNode = GraphTestUtil.createNode("node.00", "AAA");
      expectedNode.addIncomingEdgeWithTags(
          DNAStrand.FORWARD, new EdgeTerminal("in2", DNAStrand.FORWARD),
          tags2, maxThreads);

      testCase.expected.put(expectedNode.getNodeId(), expectedNode);
    }

    {
      GraphNode expectedIn = GraphTestUtil.createNode("in", "AAA");
      expectedIn.addOutgoingEdgeWithTags(
          DNAStrand.FORWARD,
          new EdgeTerminal("node.01", DNAStrand.FORWARD), tags1, maxThreads);
      testCase.expected.put(expectedIn.getNodeId(), expectedIn);
    }

    {
      GraphNode expectedIn1 = GraphTestUtil.createNode("in2", "AAA");
      expectedIn1.addOutgoingEdge(
          DNAStrand.FORWARD,
          new EdgeTerminal("node.00", DNAStrand.FORWARD));
      testCase.expected.put(expectedIn1.getNodeId(), expectedIn1);
    }

    {
      GraphNode expectedOut= GraphTestUtil.createNode("out1", "AAA");
      expectedOut.addIncomingEdgeWithTags(
          DNAStrand.FORWARD,
          new EdgeTerminal("node.01", DNAStrand.FORWARD), tags1, maxThreads);

      testCase.expected.put(expectedOut.getNodeId(), expectedOut);
    }


    return testCase;
  }

  @Test
  public void testResolveSpanningReadPaths() {
    TestCase testCase = createBasic();
    resolveSpanningReadPaths(testCase.graph, testCase.nodeId);

    assertEquals(testCase.expected.size(), testCase.graph.size());

    for (String key : testCase.expected.keySet()) {
      assertEquals(
          testCase.expected.get(key), testCase.graph.get(key));
    }
  }


  @Test
  public void testResolveCycle() {
    // Make sure that if a node has a cycle it is properly handled.
    TestCase testCase = createBasic();

    // Add a self edge to the graph.
    GraphNode node = testCase.graph.get("node");
    GraphUtil.addBidirectionalEdge(
        node, DNAStrand.FORWARD, node, DNAStrand.FORWARD);

    GraphNode expected = testCase.expected.get("node.00");
    GraphUtil.addBidirectionalEdge(
        expected, DNAStrand.FORWARD, expected, DNAStrand.FORWARD);

    resolveSpanningReadPaths(testCase.graph, testCase.nodeId);

    assertEquals(testCase.expected.size(), testCase.graph.size());

    for (String key : testCase.expected.keySet()) {
      assertEquals(
          testCase.expected.get(key), testCase.graph.get(key));
    }
  }

  /**
   * Write lists of nodes to an avro file.
   * @param avroFile
   * @param nodes
   */
  private static void writeNodeListsToFile(
      File avroFile, Collection<List<GraphNode>> lists) {
    // Write the data to the file.
    Schema schema =  Schema.createArray((new GraphNodeData()).getSchema());
    DatumWriter<List<GraphNodeData>> datumWriter =
        new SpecificDatumWriter<List<GraphNodeData>>(schema);
    DataFileWriter<List<GraphNodeData>> writer =
        new DataFileWriter<List<GraphNodeData>>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (List<GraphNode> nodes: lists) {
        List<GraphNodeData> data = new ArrayList<GraphNodeData>();
        for (GraphNode node : nodes) {
          data.add(node.getData());
        }
        writer.append(data);
      }
      writer.close();
    } catch (IOException exception) {
      fail(
          "There was a problem writing the graph to an avro file. " +
          exception.getMessage());
    }
  }

  @Test
  public void testMR() {
    // Put all the nodes in one subgraph so graph can be entirely resolved
    // in the mapper.
    TestCase testCase = createBasic();
    File tempDir = FileHelper.createLocalTempDir();
    Path avroPath = new Path(
        FilenameUtils.concat(tempDir.getPath(), "graph.avro"));

    ArrayList<List<GraphNode>> lists = new ArrayList<List<GraphNode>>();
    ArrayList<GraphNode> row = new ArrayList<GraphNode>();
    row.addAll(testCase.graph.values());
    lists.add(row);
    writeNodeListsToFile(
        new File(avroPath.toString()), lists);

    ResolveThreads stage = new ResolveThreads();
    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("inputpath", avroPath.toString());

    String outPath = FilenameUtils.concat(tempDir.getPath(), "output");
    parameters.put("outputpath", outPath);
    stage.setParameters(parameters);
    assertTrue(stage.execute());

    // Load the output.
    GraphNodeFilesIterator iterator = GraphNodeFilesIterator.fromGlob(
        new JobConf(), FilenameUtils.concat(outPath, "part-00000.avro"));

    HashMap<String, GraphNode> resolved = new HashMap<String, GraphNode>();
    for (GraphNode node : iterator) {
      resolved.put(node.getNodeId(), node.clone());
    }
    assertEquals(testCase.expected, resolved);
  }
}
