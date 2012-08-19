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
    public int bubbleLenghThreshold = 100;
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
    testCase.bubbleLenghThreshold = 2;

    GraphNode nonBubbleNode = graph.getNode(
        graph.findNodeIdForSequence("ATTC"));
    testCase.expectedMessages.put(
        nonBubbleNode.getNodeId(), nonBubbleNode.getData());
    testCase.node = nonBubbleNode.clone().getData();

    return testCase;
  }

  // In this test case, we build a node with indegree=outdegree=1 and whose
  // sequence length is < bubble length threshold so it is a potential
  // bubble.
  private MapTestCaseData createBubbleData()  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATC", 2);
    graph.addEdge("ATC", "TCA", 2);

    MapTestCaseData testCase = new MapTestCaseData();
    testCase.expectedMessages = new HashMap<String, GraphNodeData>();

    GraphNode bubbleNode = graph.findNodeForSequence("ATC");
    testCase.expectedMessages.put(
        graph.findNodeIdForSequence("TCA"), bubbleNode.clone().getData());
    testCase.node = bubbleNode.clone().getData();
    return testCase;
  }

  @Test
  public void testMap() {
    List <MapTestCaseData> cases = new ArrayList<MapTestCaseData>();
    cases.add(createBubbleData());
    cases.add(createNonBubbleData());

    ReporterMock reporter = new ReporterMock();

    FindBubblesAvro.FindBubblesAvroMapper mapper =
        new FindBubblesAvro.FindBubblesAvroMapper();
    FindBubblesAvro stage= new FindBubblesAvro();

    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    JobConf job = new JobConf(FindBubblesAvro.FindBubblesAvroMapper.class);

    for (MapTestCaseData testCase : cases) {
      definitions.get("bubble_length_threshold").addToJobConf(
          job, new Integer(testCase.bubbleLenghThreshold));
      mapper.configure(job);
      AvroCollectorMock<Pair<CharSequence, GraphNodeData>>
      collectorMock =
        new AvroCollectorMock<Pair<CharSequence, GraphNodeData>>();
      try {
        mapper.map(testCase.node, collectorMock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
      assertMapperOutput(
          testCase.node, testCase.expectedMessages, collectorMock);
    }
  }

  private static class ReduceTestCaseData {
    List<GraphNodeData> mapOutputs;
    HashMap<String, FindBubblesOutput> expectedOutputs;
    CharSequence key;
    int K;
    Float bubbleEditRate;
    public ReduceTestCaseData() {
      mapOutputs = new ArrayList<GraphNodeData>();
      expectedOutputs = new HashMap<String, FindBubblesOutput>();
      bubbleEditRate = 2.0f;
    }
  }

  private void assertReduceOutput(ReduceTestCaseData caseData,
      AvroCollectorMock<FindBubblesOutput> collectorMock) {
    assertEquals(caseData.expectedOutputs.size(), collectorMock.data.size());
    Set<String> outNodeIDList = new HashSet<String>();
    for(FindBubblesOutput element: collectorMock.data) {
      String key = null;
      if (element.getNode() != null) {
        key = element.getNode().getNodeId().toString();
      } else {
        key = element.getMinorNodeId().toString();
      }

      outNodeIDList.add(key);
      FindBubblesOutput expected = caseData.expectedOutputs.get(key);
      assertEquals(expected, element);
    }
    assertEquals(outNodeIDList, caseData.expectedOutputs.keySet());
  }

  // This function creates a test-case for a non bubble node.
  private ReduceTestCaseData constructNonBubblesCaseData()  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATATC", 2);
    ReduceTestCaseData testData = new ReduceTestCaseData();

    FindBubblesOutput output = new FindBubblesOutput();
    GraphNode node = graph.findNodeForSequence("AAT");
    output.setNode(node.getData());
    output.setMinorNodeId("");
    output.setDeletedNeighbors(new ArrayList<CharSequence>());

    testData.mapOutputs.add(
        graph.findNodeForSequence("AAT").clone().getData());

    testData.expectedOutputs = new HashMap<String, FindBubblesOutput>();
    testData.key = graph.findNodeIdForSequence("AAT");
    testData.expectedOutputs.put(
        output.getNode().getNodeId().toString(), output);

    return testData;
  }

  // This function creates a Bubble scenario where potential bubbles have been
  // shipped to the major node.
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
    List <GraphNodeData> mapOutputs = new ArrayList <GraphNodeData>();
    ReduceTestCaseData testData = new ReduceTestCaseData();
    testData.K = 3;

    mapOutputs.add(majorNode.clone().getData());
    mapOutputs.add(aliveNode.clone().getData());
    mapOutputs.add(deadNode.clone().getData());

    // Construct the expected outputs. There are three outputs.
    testData.expectedOutputs = new HashMap<String, FindBubblesOutput>();

    // For the major node (TCA) we just output the node after removing
    // the edge to ATATC.
    {
      FindBubblesOutput expectedOutput= new FindBubblesOutput();

      GraphNode node = majorNode.clone();
      node.removeNeighbor(deadNode.getNodeId());

      expectedOutput.setNode(node.getData());
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.setMinorNodeId("");
      testData.expectedOutputs.put(node.getNodeId(), expectedOutput);
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
      testData.expectedOutputs.put(node.getNodeId(), expectedOutput);
    }

    {
      // For node ATATC we output a message to AAT to remove the edge
      // to ATATC.
      FindBubblesOutput expectedOutput = new FindBubblesOutput();
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.getDeletedNeighbors().add(deadNode.getNodeId());

      expectedOutput.setMinorNodeId(minorNode.getNodeId());
      testData.expectedOutputs.put(minorNode.getNodeId(), expectedOutput);
    }

    testData.key =  majorNode.getNodeId();
    testData.mapOutputs= mapOutputs;
    return testData;
  }

  // This function creates a bubble and tests that sequences are properly
  // aligned before computing the edit distance.
  // The graph in this case is  X->{A,R(B)}->Y. If the sequences aren't
  // properly aligned (e.g. if we end up computing the edit distance of A & B
  // then the result should be two large for the nodes to be merges.
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

    // Set the bubbleEditRate to 2/5 so that distance(CTGAT, CTTAT)
    // < length * bubbleEditRate.
    testData.bubbleEditRate = 2.0f/5.0f;
    testData.key = majorNode.getNodeId();
    testData.mapOutputs = new ArrayList<GraphNodeData>();
    testData.mapOutputs.add(majorNode.clone().getData());
    testData.mapOutputs.add(highNode.clone().getData());
    testData.mapOutputs.add(lowNode.clone().getData());

    testData.expectedOutputs = new HashMap<String, FindBubblesOutput>();

    // For the major node we just output the node after removing
    // the edge to the bubble.
    {
      FindBubblesOutput expectedOutput= new FindBubblesOutput();
      GraphNode node = majorNode.clone();
      node.removeNeighbor(lowNode.getNodeId());

      expectedOutput.setNode(node.getData());
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.setMinorNodeId("");
      testData.expectedOutputs.put(node.getNodeId(), expectedOutput);
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
      testData.expectedOutputs.put(node.getNodeId(), expectedOutput);
    }

    {
      // For node ATATC we output a message to AAT to remove the edge
      // to ATATC.
      FindBubblesOutput expectedOutput = new FindBubblesOutput();
      expectedOutput.setDeletedNeighbors(new ArrayList<CharSequence>());
      expectedOutput.getDeletedNeighbors().add(lowNode.getNodeId());
      expectedOutput.setMinorNodeId(minorID.toString());

      testData.expectedOutputs.put(
          minorID.toString(), expectedOutput);
    }

    return testData;
  }

  @Test
  public void testReduce() {
    List <ReduceTestCaseData> caseData_list =
        new ArrayList<ReduceTestCaseData>();
    caseData_list.add(constructNonBubblesCaseData());
    caseData_list.add(constructBubblesCaseData());
    caseData_list.add(constructReverseBubblesCaseData());

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    FindBubblesAvro stage= new FindBubblesAvro();
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();

    JobConf job = new JobConf(FindBubblesAvro.FindBubblesAvroReducer.class);
    FindBubblesAvro.FindBubblesAvroReducer reducer =
        new FindBubblesAvro.FindBubblesAvroReducer();

    for (ReduceTestCaseData caseData : caseData_list) {
      definitions.get("bubble_edit_rate").addToJobConf(
          job, caseData.bubbleEditRate);
      definitions.get("K").addToJobConf(job, caseData.K);
      reducer.configure(job);
      AvroCollectorMock<FindBubblesOutput> collectorMock =
          new AvroCollectorMock<FindBubblesOutput>();
      try {
        CharSequence key = caseData.key;
        reducer.reduce(key, caseData.mapOutputs, collectorMock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
      assertReduceOutput(caseData, collectorMock);
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
