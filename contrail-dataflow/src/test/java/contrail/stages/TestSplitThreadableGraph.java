package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.io.AvroFileContentsIterator;
import contrail.sequences.DNAStrand;
import contrail.util.CharUtil;
import contrail.util.FileHelper;

public class TestSplitThreadableGraph {
  private static final Logger sLogger =
      Logger.getLogger(TestSplitThreadableGraph.class);

  private static class TestCase {
    public HashMap<String, GraphNode> nodes;

    // Maping from component to expected ids in that component.
    public ArrayList<ArrayList<String>> expectedOutput;

    public TestCase() {
      nodes = new HashMap<String, GraphNode>();
      expectedOutput = new ArrayList<ArrayList<String>>();
    }
  }

  private TestCase createGraph() {
    GraphNode node = GraphTestUtil.createNode("node", "AAA");
    TestCase testCase = new TestCase();
    HashMap<String, GraphNode> nodes = testCase.nodes;
    int maxThreads = 100;

    List<String> tags1 = Arrays.asList("read1");
    List<String> tags2 = Arrays.asList("read2");
    List<String> allTags = new ArrayList<String>();
    allTags.addAll(tags1);
    allTags.addAll(tags2);

    // Add two incoming edges.
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

    ArrayList<String> allIds = new ArrayList<String>();
    allIds.addAll(nodes.keySet());
    Collections.sort(allIds);
    testCase.expectedOutput.add(allIds);


    return testCase;
  }

  @Test
  public void testMain() {
    TestCase testCase = createGraph();

    File temp = FileHelper.createLocalTempDir();
    String graphPath = FilenameUtils.concat(
        temp.getAbsolutePath(), "graph.avro");

    GraphUtil.writeGraphToPath(
        new Configuration(), new Path(graphPath), testCase.nodes.values());

    // Run it.
    SplitThreadableGraph stage = new SplitThreadableGraph();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    //stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", graphPath);
    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());
    stage.setParameters(params);

    assertTrue(stage.execute());

    AvroFileContentsIterator<List<CharSequence>> outIterator
      = AvroFileContentsIterator.fromGlob(
            new Configuration(),
            FilenameUtils.concat(outputPath.toString(), "*avro"));

    ArrayList<List<String>> outputs = new ArrayList<List<String>>();
    for (List<CharSequence> outIds : outIterator) {
      outputs.add(CharUtil.toStringList(outIds));
    }
    assertEquals(1, outputs.size());
    assertEquals(testCase.expectedOutput, outputs);
    System.out.println("Files should be in:" + outputPath.toString());
  }
}
