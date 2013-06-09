/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAStrand;
import contrail.util.FileHelper;

public class TestResolveThreadsPipeline {
  private static final Logger sLogger =
      Logger.getLogger(TestResolveThreadsPipeline.class);

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
  public void testPipeline() {
    TestCase testCase = createBasic();
    File tempDir = FileHelper.createLocalTempDir();
    Path avroPath = new Path(
        FilenameUtils.concat(tempDir.getPath(), "graph.avro"));

    ArrayList<List<GraphNode>> lists = new ArrayList<List<GraphNode>>();
    ArrayList<GraphNode> row = new ArrayList<GraphNode>();
    row.addAll(testCase.graph.values());
    lists.add(row);
    GraphUtil.writeGraphToPath(
        new Configuration(), avroPath, testCase.graph.values());

    ResolveThreadsPipeline stage = new ResolveThreadsPipeline();
    HashMap<String, Object> parameters = new HashMap<String, Object>();
    stage.setParameter("inputpath", avroPath.toString());

    String outPath = FilenameUtils.concat(tempDir.getPath(), "output");
    stage.setParameter("outputpath", outPath);
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
