package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import contrail.RemoveNeighborMessage;
import contrail.ReporterMock;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

public class TestRemoveLowCoverageAvro extends RemoveLowCoverageAvro  {

  /*
   * Check the output of the map is correct.
   */
  private void assertMapperOutput(
      GraphNodeData expected_node,
      HashMap<String, RemoveNeighborMessage> expected_messages,
      AvroCollectorMock<Pair<CharSequence, RemoveNeighborMessage>> collector_mock) {

    Iterator<Pair<CharSequence, RemoveNeighborMessage>> it =
        collector_mock.data.iterator();

    // We check the sizes are equal because we want to make sure no message
    // is duplicated in the output.
    assertEquals(expected_messages.size(), collector_mock.data.size());
    while (it.hasNext()) {
      Pair<CharSequence, RemoveNeighborMessage> pair = it.next();
      String key = pair.key().toString();
      assertEquals(expected_messages.get(key), pair.value());
    }
  }

  // Store the data for a particular test case for the map phase.
  private static class MapTestCaseData {
    public GraphNodeData node;
    public HashMap<String, RemoveNeighborMessage> expected_messages;
    public Integer lengthThreshold = 5;
    public Float coverageThreshold = 4.0f;
    public Integer minLength = 0;
    public MapTestCaseData () {
      expected_messages = new HashMap<String, RemoveNeighborMessage>();
    }
  }

  // short sequence (less than threshold) but not low coverage; hence classified as Non_Low coverage node
  MapTestCaseData constructNonLowCoverageNode()  {
    MapTestCaseData high_cov= new MapTestCaseData();
    GraphNode node = new GraphNode();
    node.setCoverage(5);
    Sequence seq = new Sequence("TCA", DNAAlphabetFactory.create());
    node.setNodeId("TCA");
    node.setSequence(seq);

    RemoveNeighborMessage high_cov_msg = new RemoveNeighborMessage();
    high_cov_msg.setNode(node.getData());
    high_cov_msg.setNodeIDtoRemove("");
    HashMap<String, RemoveNeighborMessage> expected_high_cov = new HashMap<String, RemoveNeighborMessage>();
    expected_high_cov.put(node.getNodeId(), high_cov_msg);

    high_cov.node=  node.getData();
    high_cov.expected_messages= expected_high_cov;
    return high_cov;
  }

  // Short sequence (less than min_length) with high coverage.
  // This node should be removed.
 MapTestCaseData constructShortNodeTestCase()  {
   MapTestCaseData testCase= new MapTestCaseData();
   testCase.minLength = 10;
   testCase.lengthThreshold = 20;
   testCase.coverageThreshold = 5.0f;
   GraphNode node = new GraphNode();
   node.setCoverage(30);
   Sequence seq = new Sequence("TCA", DNAAlphabetFactory.create());
   node.setNodeId("TCA");
   node.setSequence(seq);

   testCase.node = node.getData();
   return testCase;
 }

  //short sequence (less than threshold) and also low coverage; hence classified Low coverage node
  MapTestCaseData constructLowCoverageNode()  {
    MapTestCaseData test_data = new MapTestCaseData();
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);
    graph.addEdge("GTC", "TCA", 2);

    GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCA"));
    node.setCoverage(3);
    // TCA is low coverage
    // send msgs to all its neighbors <nodeid_neighbor, {null, nodeid(TCA)}>

    HashMap<String, RemoveNeighborMessage> expected_low_cov = new HashMap<String, RemoveNeighborMessage>();
    GraphNode low_cov_node = graph.getNode(graph.findNodeIdForSequence("TCA"));
    {
      RemoveNeighborMessage low_cov_msg = new RemoveNeighborMessage();
      low_cov_msg.setNode(null);
      low_cov_msg.setNodeIDtoRemove(low_cov_node.getNodeId());
      expected_low_cov.put(graph.findNodeIdForSequence("AAATC"), low_cov_msg);
    }
    {
      RemoveNeighborMessage low_cov_msg = new RemoveNeighborMessage();
      low_cov_msg.setNode(null);
      low_cov_msg.setNodeIDtoRemove(low_cov_node.getNodeId());
      expected_low_cov.put(graph.findNodeIdForSequence("GTC"), low_cov_msg);
    }
    test_data.node= low_cov_node.getData();
    test_data.expected_messages= expected_low_cov;
    return test_data;
  }

  private List<MapTestCaseData> constructMapIslands()  {
    // Construct test cases where the input is an island. If the island
    // is longer than the cutoff or has higher coverage then it should be
    // removed.
    GraphNode node = new GraphNode();
    node.setSequence(new Sequence("ACTGAT", DNAAlphabetFactory.create()));
    node.setCoverage(10.0f);

    List<MapTestCaseData> cases = new ArrayList<MapTestCaseData>();

    // Construct all 4 test cases.
    for (boolean tooLong : new boolean[] {true, false}) {
      for (boolean highCoverage : new boolean[] {true, false}) {
        MapTestCaseData testCase = new MapTestCaseData();
        if (tooLong) {
          testCase.lengthThreshold = node.getSequence().size() - 2;
        } else {
          testCase.lengthThreshold = node.getSequence().size() + 2;
        }

        if (highCoverage) {
          testCase.coverageThreshold = node.getCoverage() - 2;
        } else {
          testCase.coverageThreshold = node.getCoverage() + 2;
        }
        testCase.node = node.clone().getData();

        if (tooLong || highCoverage) {
          // Island should not be removed.
          RemoveNeighborMessage message = new RemoveNeighborMessage();
          message.setNode(node.clone().getData());
          message.setNodeIDtoRemove("");
          testCase.expected_messages.put(node.getNodeId(), message);
          cases.add(testCase);
        }
      }
    }
    return cases;
  }

  // In this test case their are edges between both strands of a low coverage
  // node and its neighbor; i.e the graph is  A <->B. We want to make sure
  // that the low coverage node outputs a single remove message to its neighbor.
  MapTestCaseData constructDoubleEdge()  {
    MapTestCaseData testData = new MapTestCaseData();
    SimpleGraphBuilder graph = new SimpleGraphBuilder();

    graph.addEdge("GACCTTC", "TCA", 2);

    GraphNode lowNode = graph.getNode(graph.findNodeIdForSequence("GACCTTC"));
    GraphNode highNode = graph.getNode(graph.findNodeIdForSequence("TCA"));

    GraphUtil.addBidirectionalEdge(
        lowNode, DNAStrand.FORWARD, highNode, DNAStrand.FORWARD);

    // Check the test is setup correctly.
    for (DNAStrand strand : DNAStrand.values()) {
      Set<EdgeTerminal> terminals =
          lowNode.getEdgeTerminalsSet(strand, EdgeDirection.OUTGOING);
      assertTrue(terminals.contains(
          new EdgeTerminal(highNode.getNodeId(), DNAStrand.FORWARD)));
    }
    testData.coverageThreshold = 10.0f;
    testData.lengthThreshold = lowNode.getSequence().size() + 10;
    lowNode.setCoverage(testData.coverageThreshold - 1.0f);
    highNode.setCoverage(testData.coverageThreshold + 1.0f);

    HashMap<String, RemoveNeighborMessage> expectedOutputs =
        new HashMap<String, RemoveNeighborMessage>();
    {
      RemoveNeighborMessage message = new RemoveNeighborMessage();
      message.setNode(null);
      message.setNodeIDtoRemove(lowNode.getNodeId());
      expectedOutputs.put(highNode.getNodeId(), message);
    }
    testData.node = lowNode.clone().getData();
    testData.expected_messages = expectedOutputs;
    return testData;
  }

  @Test
  public void testMap() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    RemoveLowCoverageAvro.RemoveLowCoverageAvroMapper mapper =
        new RemoveLowCoverageAvro.RemoveLowCoverageAvroMapper();
    RemoveLowCoverageAvro stage = new RemoveLowCoverageAvro();
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();

    List <MapTestCaseData> testCases= new ArrayList<MapTestCaseData>();
    testCases.add(constructShortNodeTestCase());
    testCases.add(constructDoubleEdge());
    testCases.add(constructLowCoverageNode());
    testCases.add(constructNonLowCoverageNode());
    testCases.addAll(constructMapIslands());

    JobConf job =
        new JobConf(RemoveLowCoverageAvro.RemoveLowCoverageAvroMapper.class);

    for (MapTestCaseData case_data : testCases) {
      definitions.get("length_thresh").addToJobConf(
          job, case_data.lengthThreshold);
      definitions.get("low_cov_thresh").addToJobConf(
          job, case_data.coverageThreshold);
      definitions.get("min_length").addToJobConf(
          job, case_data.minLength);
      mapper.configure(job);

      AvroCollectorMock<Pair<CharSequence, RemoveNeighborMessage>>
      collector_mock =  new AvroCollectorMock<Pair<CharSequence, RemoveNeighborMessage>>();
      try {
        mapper.map(case_data.node, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
      assertMapperOutput(case_data.node, case_data.expected_messages, collector_mock);
    }
  }

  private static class ReduceTestCaseData {
    String key;
    List<RemoveNeighborMessage> map_out_list;
    GraphNodeData expected_node_data;

    public Integer lengthThreshold = 5;
    public Float coverageThreshold = 4.0f;

    public ReduceTestCaseData() {
      map_out_list = new ArrayList<RemoveNeighborMessage>();
    }
  }

  private void assertReduceOutput(
      ReduceTestCaseData case_data,
      AvroCollectorMock<GraphNodeData> collector_mock) {
    if (case_data.expected_node_data == null) {
      assertEquals(0, collector_mock.data.size());
      return;
    }
    assertEquals(1, collector_mock.data.size());
    assertEquals(case_data.expected_node_data, collector_mock.data.get(0));
  }

  private ReduceTestCaseData constructHighCoverageData() {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);// this is to make sure the isolated node doesn't get removed in reducer

    List <RemoveNeighborMessage> map_out_list= new ArrayList <RemoveNeighborMessage>();

    GraphNode node = graph.findNodeForSequence("TCA");
    RemoveNeighborMessage msg = new RemoveNeighborMessage();
    msg.setNode(node.clone().getData());
    msg.setNodeIDtoRemove("");
    map_out_list.add(msg);

    ReduceTestCaseData test_data= new ReduceTestCaseData();
    test_data.expected_node_data = graph.getNode(graph.findNodeIdForSequence("TCA")).clone().getData();
    test_data.map_out_list = map_out_list;
    test_data.key = node.getNodeId();
    return test_data;
  }

  private ReduceTestCaseData constructLowCoverageData() {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);
    graph.addEdge("TAA","AAATC",2); // this is to make sure the isolated node doesn't get removed in reducer
    // key AAATC gets 2 msgs
    // one msg is from itself
    // other is from low coverage node TCA

    ReduceTestCaseData test_data= new ReduceTestCaseData();
    List <RemoveNeighborMessage> map_out_list= new ArrayList <RemoveNeighborMessage>();
    {
      RemoveNeighborMessage msg = new RemoveNeighborMessage();
      msg.setNode(graph.getNode(graph.findNodeIdForSequence("AAATC")).clone().getData());
      msg.setNodeIDtoRemove("");
      map_out_list.add(msg);
    }
    {
      RemoveNeighborMessage msg = new RemoveNeighborMessage();
      msg.setNode(null);
      msg.setNodeIDtoRemove(graph.findNodeIdForSequence("TCA"));
      map_out_list.add(msg);
    }
    GraphNode node_temp = graph.getNode(graph.findNodeIdForSequence("AAATC")).clone();
    node_temp.removeNeighbor(graph.findNodeIdForSequence("TCA"));

    test_data.expected_node_data = node_temp.getData();
    test_data.map_out_list = map_out_list;
    test_data.key = "AAATC";
    return test_data;
  }

  private ReduceTestCaseData reducerNodeAlreadyRemoved() {
    // In this test case, no node is sent to the reducer because the node
    // was removed in the mapper because its coverage was too low.
    String nodeId = "nodeId";
    ReduceTestCaseData testData= new ReduceTestCaseData();
    RemoveNeighborMessage message = new RemoveNeighborMessage();
    message.setNodeIDtoRemove(nodeId);
    testData.expected_node_data = null;
    testData.map_out_list = new ArrayList<RemoveNeighborMessage>();
    testData.map_out_list.add(message);
    testData.key = nodeId;
    return testData;
  }

  private List<ReduceTestCaseData> constructReduceIslands()  {
    // Construct test cases where the input is an island. If the island
    // is longer than the cutoff or has higher coverage then it should be
    // removed.
    GraphNode node = new GraphNode();
    node.setSequence(new Sequence("ACTGAT", DNAAlphabetFactory.create()));
    node.setCoverage(10.0f);

    List<ReduceTestCaseData> cases = new ArrayList<ReduceTestCaseData>();

    // Construct all 4 test cases.
    for (boolean tooLong : new boolean[] {true, false}) {
      for (boolean highCoverage : new boolean[] {true, false}) {
        ReduceTestCaseData testCase = new ReduceTestCaseData();
        if (tooLong) {
          testCase.lengthThreshold = node.getSequence().size() - 2;
        } else {
          testCase.lengthThreshold = node.getSequence().size() + 2;
        }

        if (highCoverage) {
          testCase.coverageThreshold = node.getCoverage() - 2;
        } else {
          testCase.coverageThreshold = node.getCoverage() + 2;
        }

        RemoveNeighborMessage message = new RemoveNeighborMessage();
        message.setNode(node.clone().getData());
        message.setNodeIDtoRemove("");
        testCase.map_out_list.add(message);

        if (tooLong || highCoverage) {
          // Island should not be removed.
          testCase.expected_node_data = node.clone().getData();
        }
        cases.add(testCase);
      }
    }
    return cases;
  }

  @Test
  public void testReduce() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    JobConf job =
        new JobConf(RemoveLowCoverageAvro.RemoveLowCoverageAvroReducer.class);
    RemoveLowCoverageAvro stage = new RemoveLowCoverageAvro();
    RemoveLowCoverageAvro.RemoveLowCoverageAvroReducer reducer =
        new RemoveLowCoverageAvro.RemoveLowCoverageAvroReducer();
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();

    List<ReduceTestCaseData> testCases =
        new ArrayList<ReduceTestCaseData>();

    testCases.add(constructHighCoverageData());
    testCases.add(constructLowCoverageData());
    testCases.add(reducerNodeAlreadyRemoved());
    testCases.addAll(constructReduceIslands());

    for (ReduceTestCaseData case_data : testCases) {
      definitions.get("length_thresh").addToJobConf(
          job, case_data.lengthThreshold);
      definitions.get("low_cov_thresh").addToJobConf(
          job, case_data.coverageThreshold);
      reducer.configure(job);

      AvroCollectorMock<GraphNodeData> collector_mock =
          new AvroCollectorMock<GraphNodeData>();
      try {
        reducer.reduce(
            case_data.key, case_data.map_out_list, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
      assertReduceOutput(case_data, collector_mock);
    }
  }
}