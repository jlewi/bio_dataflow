package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.util.FileHelper;

public class TestFindBubblesAvro extends FindBubblesAvro{
  // Check the output of the map is correct.
  private void assertMapperOutput(
      GraphNodeData expected_node,
      HashMap<String, GraphNodeData> expectedMessages,
      AvroCollectorMock<Pair<CharSequence, GraphNodeData>> collectorMock) {
    // Check each output matches one of the expected outputs.
    Set<String>outNodeIDList = new HashSet<String>();
    for(Pair<CharSequence, GraphNodeData> pair: collectorMock.data) {
      String key = pair.key().toString();
      assertEquals(expectedMessages.get(key), pair.value());
      outNodeIDList.add(key);
    }
  }

  // This class stores the data for a test case for the map phase.
  private static class MapTestCaseData {
    public GraphNodeData node;
    public HashMap<String, GraphNodeData> expectedMessages;
  }

  // In this test case, we build a node with indegree=outdegree=1 but whose
  // sequence length is >= bubble length threshold so it is not eligible to be
  // a bubble.
  private MapTestCaseData createNonBubbleData() {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATTC", 2);
    graph.addEdge("ATTC", "TCA", 2);

    MapTestCaseData testCase = new MapTestCaseData();
    testCase.expectedMessages =
        new HashMap<String, GraphNodeData>();

    GraphNode nonBubbleNode = graph.getNode(
        graph.findNodeIdForSequence("ATTC"));
    testCase.expectedMessages.put(
        nonBubbleNode.getNodeId(), nonBubbleNode.getData());
    testCase.node = nonBubbleNode.clone().getData();

    return testCase;
  }

  //In this test case, we build a node with indegree=outdegree=1 but whose
  // sequence length is < bubble length threshold so it can be a potential bubble.
  private MapTestCaseData createBubbleData()  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATC", 2);
    graph.addEdge("ATC", "TCA", 2);

    MapTestCaseData bubble= new MapTestCaseData();
    HashMap<String, GraphNodeData> expected_bubble = new HashMap<String, GraphNodeData>();

    GraphNode bubble_node = graph.getNode(graph.findNodeIdForSequence("ATC"));
    expected_bubble.put(graph.findNodeIdForSequence("TCA"), bubble_node.getData());
    bubble.node = bubble_node.getData();
    bubble.expectedMessages = expected_bubble;
    return bubble;
  }

  private List<MapTestCaseData> constructMapCases() {
    List <MapTestCaseData> cases = new ArrayList<MapTestCaseData>();
    cases.add(createBubbleData());
    cases.add(createNonBubbleData());
    return cases;
  }

  @Test
  public void testMap() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    FindBubblesAvro.FindBubblesAvroMapper mapper = new FindBubblesAvro.FindBubblesAvroMapper();
    FindBubblesAvro stage= new FindBubblesAvro();

    Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
    int BubbleLenThresh= 4;
    JobConf job = new JobConf(FindBubblesAvro.FindBubblesAvroMapper.class);
    definitions.get("bubble_length_threshold").addToJobConf(job, new Integer(BubbleLenThresh));
    mapper.configure(job);

    // Construct the different test cases.
    List <MapTestCaseData> test_cases = constructMapCases();
    for (MapTestCaseData case_data : test_cases) {
      AvroCollectorMock<Pair<CharSequence, GraphNodeData>>
      collectorMock =  new AvroCollectorMock<Pair<CharSequence, GraphNodeData>>();
      try {
        mapper.map(case_data.node, collectorMock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
      assertMapperOutput(case_data.node, case_data.expectedMessages, collectorMock);
    }
  }

  private static class ReduceTestCaseData {
    List <GraphNodeData> map_out_list;
    HashMap<String,FindBubblesOutput> expected_node_data;
    CharSequence key;
    int K;
  }

  private void assertReduceOutput(ReduceTestCaseData case_data,
      AvroCollectorMock<FindBubblesOutput> collectorMock) {
    assertEquals(case_data.expected_node_data.size(), collectorMock.data.size());
    // check if all expected out puts exist
    Set<String> outNodeIDList = new HashSet<String>();
    for(FindBubblesOutput element: collectorMock.data) {
      String key = null;
      if (element.getNode() != null) {
        key = element.getNode().getNodeId().toString();
      } else {
        key = element.getMinorNodeId().toString();
      }

      outNodeIDList.add(key);
      FindBubblesOutput expected = case_data.expected_node_data.get(key);
      assertEquals(expected, element);
    }
    assertEquals(outNodeIDList, case_data.expected_node_data.keySet());
  }

  // this function creates a test-case for a Non Bubble node
  private ReduceTestCaseData constructNonBubblesCaseData()  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATATC", 2);
    List <GraphNodeData> map_out_list = new ArrayList <GraphNodeData>();
    ReduceTestCaseData testData = new ReduceTestCaseData();
    FindBubblesOutput expected_node_data = new FindBubblesOutput();


    GraphNode node = graph.findNodeForSequence("AAT");
    expected_node_data.setNode(node.getData());
    expected_node_data.setMinorNodeId("");
    expected_node_data.setDeletedNeighbors(new ArrayList<CharSequence>());

    GraphNodeData msg = new GraphNodeData();
    msg = graph.getNode(graph.findNodeIdForSequence("AAT")).getData();
    map_out_list.add(msg);

    testData.expected_node_data = new HashMap<String, FindBubblesOutput>();
    testData.key = graph.findNodeIdForSequence("AAT");
    testData.expected_node_data.put(
        expected_node_data.getNode().getNodeId().toString(),expected_node_data);
    testData.map_out_list = map_out_list;
    return testData;
  }

  // this function creates a Bubble scenario where potential bubbles have been
  // shipped to the major node
  private ReduceTestCaseData constructBubblesCaseData()  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATATC", 2);
    graph.addEdge("AAT", "ATTTC", 2);
    graph.addEdge("ATTTC", "TCA", 2);
    graph.addEdge("ATATC", "TCA", 2);

    GraphNode majorNode = graph.getNode(graph.findNodeIdForSequence("TCA"));
    // Nodes to keep and remove
    GraphNode aliveNode = graph.getNode(graph.findNodeIdForSequence("ATTTC"));
    GraphNode deadNode = graph.getNode(graph.findNodeIdForSequence("ATATC"));
    GraphNode minorNode = graph.getNode(graph.findNodeIdForSequence("AAT"));

    // We need to set the coverage for nodes ATATC, and ATTTC respectively so
    // that the node ATTTC will be kept and ATATC will be removed.
    aliveNode.setCoverage(4);
    deadNode.setCoverage(2);

    // 3 input mapper msgs
    // nodeid(TCA), <TCA nodedata>
    // nodeid(TCA), <ATTTC nodedata>
    // nodeid(TCA), <ATATC nodedata>
    List <GraphNodeData> map_out_list = new ArrayList <GraphNodeData>();
    ReduceTestCaseData testData = new ReduceTestCaseData();
    testData.K = 3;

    map_out_list.add(majorNode.clone().getData());
    map_out_list.add(aliveNode.clone().getData());
    map_out_list.add(deadNode.clone().getData());

    // Construct the expected outputs. There are three outputs.
    testData.expected_node_data = new HashMap<String, FindBubblesOutput>();

    // For the major node (TCA) we just output the node after removing
    // the edge to ATATC.
    {
      FindBubblesOutput expectedOutput= new FindBubblesOutput();

      GraphNode node = majorNode.clone();
      node.removeNeighbor(deadNode.getNodeId());

      expectedOutput.setNode(node.getData());
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.setMinorNodeId("");
      testData.expected_node_data.put(node.getNodeId(), expectedOutput);
    }
    {
      // For node ATTTC we just output the node after updating the coverage.
      FindBubblesOutput expectedOutput= new FindBubblesOutput();
      GraphNode node = aliveNode.clone();
      int aliveLength = node.getData().getSequence().getLength()
                        - testData.K + 1;
      int deadLength = deadNode.getData().getSequence().getLength()
                       - testData.K + 1;
      float extraCoverage = deadNode.getCoverage() * deadLength;
      float support = node.getCoverage() * aliveLength + extraCoverage;
      node.setCoverage(support / aliveLength);

      expectedOutput.setNode(node.clone().getData());
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.setMinorNodeId("");
      testData.expected_node_data.put(node.getNodeId(), expectedOutput);

    }

    {
      // For node ATATC we output a message to AAT to remove the edge
      // to ATATC.
      FindBubblesOutput expectedOutput = new FindBubblesOutput();
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.getDeletedNeighbors().add(deadNode.getNodeId());

      expectedOutput.setMinorNodeId(minorNode.getNodeId());
      testData.expected_node_data.put(minorNode.getNodeId(), expectedOutput);
    }

    testData.key =  majorNode.getNodeId();
    testData.map_out_list= map_out_list;
    return testData;
  }

  // this function creates a Bubble scenario where we test the proper alignment
  // and edit distance condition
  // This bubble test the case of X->{A,R(B)}->Y
  private ReduceTestCaseData constructReverseBubblesCaseData()  {
    // The reducer takes as input nodes X, A, B. So we don't construct
    // node Y.
    GraphNode majorNode = new GraphNode();
    majorNode.setCoverage(0);
    majorNode.setSequence(new Sequence("ACT", DNAAlphabetFactory.create()));

    // We set the id's such that nodeX is the major id.
    majorNode.setNodeId("bmajorId");

    GraphNode highNode = new GraphNode();    // higher coverage
    highNode.setCoverage(4);
    highNode.setNodeId("CTGAT");
    highNode.setSequence(new Sequence("CTGAT", DNAAlphabetFactory.create()));

    GraphNode lowNode = new GraphNode();
    lowNode.setCoverage(2);
    Sequence lowSequence = new Sequence("CTTAT", DNAAlphabetFactory.create());
    lowNode.setSequence(DNAUtil.reverseComplement(lowSequence));
    lowNode.setNodeId("ATAAG");

    String minorID = "aminorId";

    EdgeTerminal majorTerminal = new EdgeTerminal(
        majorNode.getNodeId(), DNAStrand.FORWARD);
    EdgeTerminal minorTerminal = new EdgeTerminal(minorID, DNAStrand.FORWARD);
    EdgeTerminal highTerminal = new EdgeTerminal(
        highNode.getNodeId(), DNAStrand.FORWARD);
    EdgeTerminal lowTerminal = new EdgeTerminal(
        lowNode.getNodeId(), DNAStrand.REVERSE);

    majorNode.addOutgoingEdge(DNAStrand.FORWARD, highTerminal);
    highNode.addIncomingEdge(highTerminal.strand, majorTerminal);

    majorNode.addOutgoingEdge(DNAStrand.FORWARD, lowTerminal);
    lowNode.addIncomingEdge(lowTerminal.strand, majorTerminal);

    highNode.addOutgoingEdge(highTerminal.strand, minorTerminal);
    lowNode.addOutgoingEdge(lowTerminal.strand, minorTerminal);

    // Construct the test case
    ReduceTestCaseData testData = new ReduceTestCaseData();
    testData.key = majorNode.getNodeId();
    testData.map_out_list = new ArrayList<GraphNodeData>();
    testData.map_out_list.add(majorNode.clone().getData());
    testData.map_out_list.add(highNode.clone().getData());
    testData.map_out_list.add(lowNode.clone().getData());

    testData.expected_node_data = new HashMap<String, FindBubblesOutput>();

    // For the major node we just output the node after removing
    // the edge to the bubble.
    {
      FindBubblesOutput expectedOutput= new FindBubblesOutput();
      GraphNode node = majorNode.clone();
      node.removeNeighbor(lowNode.getNodeId());

      expectedOutput.setNode(node.getData());
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.setMinorNodeId("");
      testData.expected_node_data.put(node.getNodeId(), expectedOutput);
    }
    {
      FindBubblesOutput expectedOutput= new FindBubblesOutput();
      GraphNode node = highNode.clone();

      int aliveLength = node.getData().getSequence().getLength()
                       - testData.K + 1;
      int deadLength = lowNode.getData().getSequence().getLength()
                       - testData.K + 1;
      float extraCoverage = lowNode.getCoverage() * deadLength;
      float support = node.getCoverage() * aliveLength + extraCoverage;
      node.setCoverage(support / aliveLength);
      expectedOutput.setNode(node.clone().getData());
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.setMinorNodeId("");
      testData.expected_node_data.put(node.getNodeId(), expectedOutput);

    }

    {
      // For node ATATC we output a message to AAT to remove the edge
      // to ATATC.
      FindBubblesOutput expectedOutput = new FindBubblesOutput();
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.getDeletedNeighbors().add(lowNode.getNodeId());
      expectedOutput.setMinorNodeId(minorID.toString());

      testData.expected_node_data.put(
          minorID.toString(), expectedOutput);
    }

    return testData;
  }

  @Test
  public void testReduce() {
    List <ReduceTestCaseData> case_data_list =
        new ArrayList<ReduceTestCaseData>();
    case_data_list.add(constructNonBubblesCaseData());
    case_data_list.add(constructBubblesCaseData());
    case_data_list.add(constructReverseBubblesCaseData());

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    FindBubblesAvro stage= new FindBubblesAvro();
    Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
    int BubbleEditRate = 2;
    JobConf job = new JobConf(FindBubblesAvro.FindBubblesAvroReducer.class);
    definitions.get("bubble_edit_rate").addToJobConf(job, new Integer(BubbleEditRate));

    FindBubblesAvro.FindBubblesAvroReducer reducer = new FindBubblesAvro.FindBubblesAvroReducer();

    for (ReduceTestCaseData case_data : case_data_list) {
      definitions.get("K").addToJobConf(job, case_data.K);
      reducer.configure(job);
      AvroCollectorMock<FindBubblesOutput> collectorMock =
          new AvroCollectorMock<FindBubblesOutput>();
      try {
        CharSequence key = case_data.key;
        reducer.reduce(key, case_data.map_out_list, collectorMock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
      assertReduceOutput(case_data, collectorMock);
    }
  }


  @Test
  public void testRun() {
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    // Create a graph with some bubbles.
    int K = 3;
    builder.addEdge("ACT", "CTATG", K - 1);
    builder.addEdge("ACT", "CTTTG", K - 1);
    builder.addEdge("CTATG", "TGA", K - 1);
    builder.addEdge("CTTTG", "TGA", K - 1);

    File tempDir = FileHelper.createLocalTempDir();
    File avroFile = new File(tempDir, "graph.avro");

    GraphUtil.writeGraphToFile(avroFile, builder.getAllNodes().values());

    // Run it.
    FindBubblesAvro stage = new FindBubblesAvro();
    File outputPath = new File(tempDir, "output");
    String[] args =
      {"--inputpath=" + tempDir.toURI().toString(),
       "--outputpath=" + outputPath.toURI().toString(),
       "--K=" + K, "--bubble_edit_rate=1", "--bubble_length_threshold=10"
      };
    try {
      stage.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
