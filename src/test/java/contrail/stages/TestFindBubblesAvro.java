package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

public class TestFindBubblesAvro extends FindBubblesAvro{
  //Check the output of the map is correct.
  private void assertMapperOutput(
      GraphNodeData expected_node, HashMap<String, GraphNodeData> expected_message,
      AvroCollectorMock<Pair<CharSequence, GraphNodeData>> collector_mock) {

    // check if all expected mapper data keys are same as keys in collected mapper
    Set<String>outNodeIDList = new HashSet<String>();
    for(Pair<CharSequence, GraphNodeData> element: collector_mock.data) {
      outNodeIDList.add(element.key().toString());
    }
    assertEquals(outNodeIDList, expected_message.keySet());
    // check if all expected data is identical to collected mapper data
    Iterator<Pair<CharSequence, GraphNodeData>> it = collector_mock.data.iterator();
    GraphNode temp_node = new GraphNode();
    temp_node.setData(expected_node);
    while (it.hasNext()) {
      Pair<CharSequence, GraphNodeData> pair = it.next();
      String key = pair.key().toString();
      assertEquals(expected_message.get(key), pair.value());
    }
  }

  // Store the data for a particular test case for the map phase.
  private static class MapTestCaseData {
    public GraphNodeData node;
    public HashMap<String, GraphNodeData> expected_message;
  }

  // In this test case, we build a node with indegree=outdegree=1 but whose
  // sequence length is >= bubble length threshold so it is not eligible to be a bubble.
  private MapTestCaseData createNonBubbleData() {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATTC", 2);
    graph.addEdge("ATTC", "TCA", 2);

    MapTestCaseData non_bubble= new MapTestCaseData();
    HashMap<String, GraphNodeData> expected_non_bubble= new HashMap<String, GraphNodeData>();

    GraphNode non_bubble_node = graph.getNode(graph.findNodeIdForSequence("ATTC"));
    expected_non_bubble.put(non_bubble_node.getNodeId(), non_bubble_node.getData());
    non_bubble.node = non_bubble_node.getData();
    non_bubble.expected_message = expected_non_bubble;
    return non_bubble;
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
    bubble.expected_message = expected_bubble;
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
      collector_mock =  new AvroCollectorMock<Pair<CharSequence, GraphNodeData>>();
      try {
        mapper.map(case_data.node, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
      assertMapperOutput(case_data.node, case_data.expected_message, collector_mock);
    }
  }

  private static class ReduceTestCaseData {
    List <GraphNodeData> map_out_list;
    HashMap<String,FindBubblesOutput> expected_node_data;
    CharSequence key;
  }

  private void assertReduceOutput(ReduceTestCaseData case_data,
      AvroCollectorMock<FindBubblesOutput> collector_mock) {
    assertEquals(case_data.expected_node_data.size(), collector_mock.data.size());
    // check if all expected out puts exist
    Set<String>outNodeIDList = new HashSet<String>();
    for(FindBubblesOutput element: collector_mock.data) {
      outNodeIDList.add(element.getNode().getNodeId().toString());
    }
    assertEquals(outNodeIDList, case_data.expected_node_data.keySet());
    // check if each reducer output is identical to expected output
    for(FindBubblesOutput data: collector_mock.data)  {
      assertEquals(case_data.expected_node_data.get(data.getNode().getNodeId().toString()), data);
    }
  }

  // this function creates a test-case for a Non Bubble node
  private ReduceTestCaseData constructNonBubblesCaseData()  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATATC", 2);
    List <GraphNodeData> map_out_list = new ArrayList <GraphNodeData>();
    ReduceTestCaseData test_data = new ReduceTestCaseData();
    FindBubblesOutput expected_node_data = new FindBubblesOutput();
    List<BubbleInfo> info_list = new ArrayList<BubbleInfo>();

    GraphNode node = graph.getNode(graph.findNodeIdForSequence("AAT"));
    expected_node_data.setNode(node.getData());
    expected_node_data.setNodeBubbleinfo(info_list);

    GraphNodeData msg = new GraphNodeData();
    msg = graph.getNode(graph.findNodeIdForSequence("AAT")).getData();
    map_out_list.add(msg);

    test_data.expected_node_data = new HashMap<String, FindBubblesOutput>();
    test_data.key = graph.findNodeIdForSequence("AAT");
    test_data.expected_node_data.put(
        expected_node_data.getNode().getNodeId().toString(),expected_node_data);
    test_data.map_out_list = map_out_list;
    return test_data;
  }

  // this function creates a Bubble scenario where potential bubbles have been
  // shipped to the major node
  private ReduceTestCaseData constructBubblesCaseData()  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAT", "ATATC", 2);
    graph.addEdge("AAT", "ATTTC", 2);
    graph.addEdge("ATTTC", "TCA", 2);
    graph.addEdge("ATATC", "TCA", 2);

    // Construct the list of test cases
    // Output of the Mapper is in the form {Key, Message <GraphNodeData>}; mapper outputs are as follows
    {
      GraphNode dead_node = new GraphNode();
      dead_node.setData(graph.getNode(graph.findNodeIdForSequence("ATATC")).getData());  // this will be removed
      dead_node.setCoverage(2);
    }
    {
      GraphNode alive_node = new GraphNode();
      alive_node.setData(graph.getNode(graph.findNodeIdForSequence("ATTTC")).getData());   // this will be left as it has higher coverage
      alive_node.setCoverage(4);
    }

    // 3 input mapper msgs
    // nodeid(TCA), <TCA nodedata>
    // nodeid(TCA), <ATTTC nodedata>
    // nodeid(TCA), <ATATC nodedata>
    List <GraphNodeData> map_out_list = new ArrayList <GraphNodeData>();
    ReduceTestCaseData test_data = new ReduceTestCaseData();

    GraphNodeData msg = new GraphNodeData();
    msg = graph.getNode(graph.findNodeIdForSequence("TCA")).getData();
    map_out_list.add(msg);

    GraphNodeData msg2 = new GraphNodeData();
    msg2 = graph.getNode(graph.findNodeIdForSequence("ATTTC")).getData();
    map_out_list.add(msg2);

    GraphNodeData msg3 = new GraphNodeData();
    msg3 = graph.getNode(graph.findNodeIdForSequence("ATATC")).getData();
    map_out_list.add(msg3);

    // FindBubblesOutput = GraphNodeData+BubbleInfo
    // Major Node, GraphNodeData+BubbleInfo
    test_data.expected_node_data = new HashMap<String, FindBubblesOutput>();
    {
      FindBubblesOutput expected_node_data = new FindBubblesOutput();
      List<BubbleInfo> info_list = new ArrayList<BubbleInfo>();

      GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCA")).clone();
      node.removeNeighbor("ATATC");
      BubbleInfo info = new BubbleInfo();
      info.setAliveNodeID(graph.findNodeIdForSequence("ATTTC"));
      info.setNodetoRemoveID(graph.getNode(graph.findNodeIdForSequence("ATATC")).getNodeId());
      info.setExtraCoverage((float) 8);
      info.setTargetID(graph.findNodeIdForSequence("AAT"));

      info_list.add(info);
      expected_node_data.setNode(node.getData());
      expected_node_data.setNodeBubbleinfo(info_list);
      test_data.expected_node_data.put(
          expected_node_data.getNode().getNodeId().toString(),expected_node_data);
    }
    // Alive escaped node, GraphNodeData+empty BubbleInfo
    {
      FindBubblesOutput expected_node_data = new FindBubblesOutput();
      List<BubbleInfo> info_list = new ArrayList<BubbleInfo>();
      BubbleInfo info = new BubbleInfo();
      info.setAliveNodeID("");
      info.setNodetoRemoveID("");
      info.setExtraCoverage((float) 0);
      info.setTargetID("");
      info_list.add(info);
      expected_node_data.setNode(graph.getNode(graph.findNodeIdForSequence("ATTTC")).getData());
      expected_node_data.setNodeBubbleinfo(info_list);

      test_data.expected_node_data.put(
          graph.findNodeIdForSequence("ATTTC").toString(),
          expected_node_data);
    }
    test_data.key =  graph.findNodeIdForSequence("TCA");
    test_data.map_out_list= map_out_list;
    return test_data;
  }

  // this function creates a Bubble scenario where we test the proper alignment
  // and edit distance condition
  // This bubble test the case of X->{A,R(B)}->Y
  private ReduceTestCaseData constructReverseBubblesCaseData()  {
    // The reducer takes as input nodes X, A, B. So we don't construct
    // node Y.
    GraphNode nodeX = new GraphNode();
    nodeX.setCoverage(0);
    Sequence seq1  = new Sequence("ACT", DNAAlphabetFactory.create());

    // We set the id's such that nodeX is the major id.
    nodeX.setNodeId("bmajorId");
    nodeX.setSequence(seq1);

//    GraphNode nodeY = new GraphNode();
//    nodeY.setCoverage(0);
//    Sequence seq2  = new Sequence("ATT", DNAAlphabetFactory.create());
//    nodeY.setNodeId("ATT");
//    nodeY.setSequence(seq2);

    GraphNode nodeA = new GraphNode();    // higher coverage
    nodeA.setCoverage(4);
    Sequence seq3  = new Sequence("CTGAT", DNAAlphabetFactory.create());
    nodeA.setNodeId("CTGAT");
    nodeA.setSequence(seq3);

    GraphNode nodeB = new GraphNode();
    nodeB.setCoverage(2);
    Sequence seq4  = new Sequence("CTTAT", DNAAlphabetFactory.create());
    nodeB.setNodeId("ATAAG");
    nodeB.setSequence(DNAUtil.reverseComplement(seq4));

    String nodeYID = "aminorId";

    EdgeTerminal nodeXTerminal = new EdgeTerminal(nodeX.getNodeId(), DNAStrand.FORWARD);
    EdgeTerminal nodeYTerminal = new EdgeTerminal(nodeYID, DNAStrand.FORWARD);

    {
      // ACT->CTGAT
      EdgeTerminal nodeATerminal = new EdgeTerminal(nodeA.getNodeId(), DNAStrand.FORWARD);


      nodeX.addOutgoingEdge(DNAStrand.FORWARD, nodeATerminal);
      nodeA.addIncomingEdge(DNAStrand.FORWARD, nodeXTerminal);

      // CTGAT->ATT



      nodeA.addOutgoingEdge(DNAStrand.FORWARD, nodeYTerminal);
      //nodeY.addIncomingEdge(DNAStrand.FORWARD, nodeXTerminal);
    }


    {
      // ACT->R(ATAAG)
      EdgeTerminal nodeBTerminal = new EdgeTerminal(nodeB.getNodeId().toString(), DNAStrand.REVERSE);
      nodeX.addOutgoingEdge(DNAStrand.FORWARD, nodeBTerminal);
      nodeB.addIncomingEdge(DNAStrand.FORWARD, nodeXTerminal);

      // R(ATAAG)->AAT
      //EdgeTerminal nodeYTerminal = new EdgeTerminal(nodeY.getNodeId().toString(), DNAStrand.FORWARD);

      nodeB.addOutgoingEdge(DNAStrand.FORWARD, nodeYTerminal);
      //nodeY.addIncomingEdge(DNAStrand.FORWARD, nodeBTerminal);
    }


    // Construct the list of test cases
    // Output of the Mapper is in the form {Key, Message <GraphNodeData>}; mapper outputs are as follows

    // 3 input mapper msgs
    // nodeid(ACT), <ACT nodedata>
    // nodeid(ACT), <CTGAT nodedata>
    // nodeid(ACT), <ATAAG nodedata>
    List <GraphNodeData> map_out_list = new ArrayList <GraphNodeData>();
    ReduceTestCaseData test_data = new ReduceTestCaseData();

    GraphNodeData msg = new GraphNodeData();
    msg = nodeX.getData();
    map_out_list.add(msg);

    GraphNodeData msg2 = new GraphNodeData();
    msg2 = nodeA.clone().getData();
    map_out_list.add(msg2);

    GraphNodeData msg3 = new GraphNodeData();
    msg3 = nodeB.clone().getData();
    map_out_list.add(msg3);

    test_data.expected_node_data = new HashMap<String, FindBubblesOutput>();
    {
      // For nodeX we should output nodeX but with the edge to nodeB removed.
      FindBubblesOutput expected_node_data = new FindBubblesOutput();
      List<BubbleInfo> info_list = new ArrayList<BubbleInfo>();

      // nodeX is major node.
      nodeX.removeNeighbor(nodeB.getNodeId());
      BubbleInfo info = new BubbleInfo();
      info.setAliveNodeID(nodeA.getNodeId());

      info.setNodetoRemoveID(nodeB.getNodeId());
      info.setExtraCoverage((float) 8);
      info.setTargetID(nodeYID);

      info_list.add(info);
      expected_node_data.setNode(nodeX.getData());
      expected_node_data.setNodeBubbleinfo(info_list);
      test_data.expected_node_data.put(
          expected_node_data.getNode().getNodeId().toString(),expected_node_data);
    }
    // Alive escaped node, GraphNodeData+empty BubbleInfo
    {
      FindBubblesOutput expected_node_data = new FindBubblesOutput();
      List<BubbleInfo> info_list = new ArrayList<BubbleInfo>();
      BubbleInfo info = new BubbleInfo();
      info.setAliveNodeID("");
      info.setNodetoRemoveID("");
      info.setExtraCoverage((float) 0);
      info.setTargetID("");
      info_list.add(info);
      expected_node_data.setNode(nodeA.getData());
      expected_node_data.setNodeBubbleinfo(info_list);

      test_data.expected_node_data.put(
          nodeA.getNodeId().toString(),
          expected_node_data);
    }
    test_data.key =  nodeX.getNodeId();
    test_data.map_out_list= map_out_list;
    return test_data;
  }

  @Test
  public void testReduce() {
    List <ReduceTestCaseData> case_data_list =
        new ArrayList<ReduceTestCaseData>();
    //case_data_list.add(constructReverseBubblesCaseData());
    //case_data_list.add(constructNonBubblesCaseData());
    case_data_list.add(constructBubblesCaseData());
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    FindBubblesAvro stage= new FindBubblesAvro();
    Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
    int BubbleEditRate = 2;
    JobConf job = new JobConf(FindBubblesAvro.FindBubblesAvroReducer.class);
    definitions.get("bubble_edit_rate").addToJobConf(job, new Integer(BubbleEditRate));
    definitions.get("K").addToJobConf(job, new Integer("2"));
    FindBubblesAvro.FindBubblesAvroReducer reducer = new FindBubblesAvro.FindBubblesAvroReducer();
    reducer.configure(job);

    for (ReduceTestCaseData case_data : case_data_list) {

      AvroCollectorMock<FindBubblesOutput> collector_mock =
          new AvroCollectorMock<FindBubblesOutput>();
      try {
        CharSequence key = case_data.key;
        reducer.reduce(key, case_data.map_out_list, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
      assertReduceOutput(case_data, collector_mock);
    }
  }
  /*
  @Test
  public void testRun() {
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGG", 3);
    File temp = null;
    try {
      temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +exception.getMessage());
    }

    if(!(temp.delete())){
      throw new RuntimeException(
          "Could not delete temp file: " + temp.getAbsolutePath());
    }
    if(!(temp.mkdir())) {
      throw new RuntimeException(
          "Could not create temp directory: " + temp.getAbsolutePath());
    }
    File avro_file = new File(temp, "graph.avro");
    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datum_writer =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datum_writer);
    try {
      writer.create(schema, avro_file);
      for (GraphNode node: builder.getAllNodes().values()) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. " +
          "Exception:" + exception.getMessage());
    }
    // Run it.
    FindBubblesAvro run_bubbles = new FindBubblesAvro();
    File output_path = new File(temp, "output");
    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
        "--outputpath=" + output_path.toURI().toString(),
        // TODO: we use 3 more parameters in findBubbles BubbleEditRate, BubbleLenThresh and K
        // do we pass them here
      };
    try {
      run_bubbles.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
   */
}
