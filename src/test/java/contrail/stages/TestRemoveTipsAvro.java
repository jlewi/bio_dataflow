package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.RemoveTipMessage;
import contrail.ReporterMock;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.StrandsForEdge;

public class TestRemoveTipsAvro extends RemoveTipsAvro{

  /*
   * Check the output of the map is correct.
   */
  private void assertMapperOutput(
      GraphNodeData expected_node, Pair<String, RemoveTipMessage> expected_message,
      AvroCollectorMock<Pair<CharSequence, RemoveTipMessage>> collector_mock) {

    // Check the output.
    Iterator<Pair<CharSequence, RemoveTipMessage>> it = collector_mock.data.iterator();

    assertTrue(it.hasNext());
    Pair<CharSequence, RemoveTipMessage> actual_message = it.next();
    assertEquals(expected_message.value(), actual_message.value());
    assertFalse(it.hasNext());
  }

  // Store the data for a particular test case for the map phase.
  private static class MapTestCaseData {
    public GraphNodeData node;
    public Pair<String, RemoveTipMessage> expected_message;
  }

  private List<MapTestCaseData> constructMapCases() {

    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);
    graph.addEdge("ATC", "TCA", 2);

    // Construct the list of test cases
    List <MapTestCaseData> cases= new ArrayList<MapTestCaseData>();

    // ADDING Non-Tip to casedata
    {
      MapTestCaseData non_tip= new MapTestCaseData();
      Pair<String, RemoveTipMessage> expected_non_tip= new Pair<String, RemoveTipMessage>(MAP_OUT_SCHEMA);
      GraphNode non_tip_node = graph.getNode(graph.findNodeIdForSequence("TCA"));

      RemoveTipMessage non_tip_msg = new RemoveTipMessage();
      non_tip_msg.setNode(non_tip_node.getData());
      non_tip_msg.setEdgeStrands(null);
      expected_non_tip.set(non_tip_node.getNodeId(), non_tip_msg);

      non_tip.node= non_tip_node.getData();
      non_tip.expected_message= expected_non_tip;
      cases.add(non_tip);
    }
    // tiplength is 4 for this case
    // ADDING Non-Tip to casedata; AAATC gets identified as NON tip as its len is > 4
    {
      MapTestCaseData non_tip= new MapTestCaseData();
      Pair<String, RemoveTipMessage> expected_non_tip= new Pair<String, RemoveTipMessage>(MAP_OUT_SCHEMA);
      GraphNode non_tip_node = graph.getNode(graph.findNodeIdForSequence("AAATC"));

      RemoveTipMessage non_tip_msg = new RemoveTipMessage();
      non_tip_msg.setNode(non_tip_node.getData());
      non_tip_msg.setEdgeStrands(null);
      expected_non_tip.set(non_tip_node.getNodeId(), non_tip_msg);

      non_tip.node= non_tip_node.getData();
      non_tip.expected_message= expected_non_tip;

      cases.add(non_tip);
    }
    // ADDING Tip to casedata
    // ATC is a tip node
    {
      MapTestCaseData tip= new MapTestCaseData();
      Pair<String, RemoveTipMessage> expected_tip= new Pair<String, RemoveTipMessage>(MAP_OUT_SCHEMA);
      GraphNode tip_node = graph.getNode(graph.findNodeIdForSequence("ATC"));

      String terminal_nodeId = graph.getNode(graph.findNodeIdForSequence("TCA")).getNodeId();
      RemoveTipMessage tip_msg = new RemoveTipMessage();
      tip_msg.setNode(tip_node.getData());
      tip_msg.setEdgeStrands(StrandsForEdge.FF);
      expected_tip.set(terminal_nodeId, tip_msg);		// if tip then output nodeID of terminal

      tip.node= tip_node.getData();
      tip.expected_message= expected_tip;

      cases.add(tip);
    }
    return cases;
  }

  @Test
  public void testMap() {

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    RemoveTipsAvro.RemoveTipsAvroMapper mapper = new RemoveTipsAvro.RemoveTipsAvroMapper();

    RemoveTipsAvro stage= new RemoveTipsAvro();
    Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
    int tiplength= 4;
    JobConf job = new JobConf(RemoveTipsAvro.RemoveTipsAvroMapper.class);
    definitions.get("tiplength").addToJobConf(job, new Integer(tiplength));
    mapper.configure(job);

    // Construct the different test cases.
    List <MapTestCaseData> test_cases = constructMapCases();

    for (MapTestCaseData case_data : test_cases) {

      AvroCollectorMock<Pair<CharSequence, RemoveTipMessage>>
      collector_mock =  new AvroCollectorMock<Pair<CharSequence, RemoveTipMessage>>();
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
    List <RemoveTipMessage> mapOutList;
    String reducer_input_key;
    HashMap<String,GraphNodeData> expected_node_data;
  }

  private void assertReduceOutput(
      ReduceTestCaseData case_data,
      AvroCollectorMock<GraphNodeData> collector_mock) {

    assertEquals(collector_mock.data.size(), case_data.expected_node_data.size());
    // check if all emitted nodeid's are in expected Key set
    Set<String>outNodeIDList = new HashSet<String>();
    for(GraphNodeData element: collector_mock.data) {
      outNodeIDList.add(element.getNodeId().toString());
    }
    assertEquals(outNodeIDList, case_data.expected_node_data.keySet());
    for(GraphNodeData element: collector_mock.data) {
      assertEquals(element, case_data.expected_node_data.get(element.getNodeId().toString()));
    }
  }

  /*
   * In this test case, we have a terminal with N tips where N=degree. Furthermore,
   * there is more than 1 tip of length L where L is the longest length of the tips.
   * In this case, all but one of the longest tips should be removed.
   */
  ReduceTestCaseData createEqualLengthTipsTestData()
  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("ATC", "TCA", 2);
    graph.addEdge("GTC", "TCA", 2);

    ReduceTestCaseData testData= new ReduceTestCaseData();
    List <RemoveTipMessage> mapOutList= new ArrayList <RemoveTipMessage>();
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCA")).clone();
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      mapOutList.add(msg);
    }
    // nodeid(TCA), <ATC, FRStrand>
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("ATC")).clone();
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      msg.setEdgeStrands(StrandsForEdge.FF);
      mapOutList.add(msg);
    }
    // nodeid(TCA), <GTC, FRStrand>
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("GTC")).clone();
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      msg.setEdgeStrands(StrandsForEdge.RF);
      mapOutList.add(msg);
    }

    GraphNode temp= graph.getNode(graph.findNodeIdForSequence("TCA")).clone();
    temp.removeNeighbor(graph.getNode(graph.findNodeIdForSequence("GTC")).getNodeId());

    testData.expected_node_data = new HashMap<String,GraphNodeData>();
    testData.expected_node_data.put(temp.getNodeId(), temp.getData());
    testData.expected_node_data.put(graph.findNodeIdForSequence("ATC"), graph.getNode(graph.findNodeIdForSequence("ATC")).getData());

    testData.mapOutList= mapOutList;
    testData.reducer_input_key = temp.getNodeId();

    return testData;
  }

  /*
   *  If we have 2 tips with same terminal AND
   *  terminal has numTips != degree then
   *  we remove all Tips
   */
  ReduceTestCaseData createNonEqualLenTipsTestData()
  {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);
    graph.addEdge("ATC", "TCA", 2);
    graph.addEdge("GTC", "TCA", 2);
    /* AAATC is not a tip because it is larger than presumed Tiplength
     * for this test case that is 4
     * hence it won't be output as tips in mapper
    */
    ReduceTestCaseData testData= new ReduceTestCaseData();
    List <RemoveTipMessage> mapOutList= new ArrayList <RemoveTipMessage>();
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCA")).clone();
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      mapOutList.add(msg);
    }
    // nodeid(TCA), <ATC, FRStrand>
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("ATC")).clone();
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      msg.setEdgeStrands(StrandsForEdge.FR);
      mapOutList.add(msg);
    }
    // nodeid(TCA), <GTC, FRStrand>
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("GTC")).clone();
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      msg.setEdgeStrands(StrandsForEdge.FR);
      mapOutList.add(msg);
    }
    // both messages are now in a List; we now set the expected node/ key for those msg
    GraphNode temp= graph.getNode(graph.findNodeIdForSequence("TCA")).clone();
    temp.removeNeighbor(graph.getNode(graph.findNodeIdForSequence("GTC")).getNodeId());
    temp.removeNeighbor(graph.getNode(graph.findNodeIdForSequence("ATC")).getNodeId());

    testData.expected_node_data = new HashMap<String,GraphNodeData>();
    testData.expected_node_data.put(temp.getNodeId(), temp.getData());
    testData.mapOutList= mapOutList;
    testData.reducer_input_key = temp.getNodeId();

    return testData;
  }

  private List<ReduceTestCaseData> constructReduceTips() {
    List<ReduceTestCaseData> testData_list = new ArrayList<ReduceTestCaseData> ();

    ReduceTestCaseData equaLenTestData = createEqualLengthTipsTestData();
    ReduceTestCaseData nonEqualLenTestData = createNonEqualLenTipsTestData();
    // add key/msg list to Object List
    testData_list.add(equaLenTestData);
    testData_list.add(nonEqualLenTestData);
    return testData_list;
  }

  @Test
  public void testReduce() {
    List <ReduceTestCaseData> case_data_list= constructReduceTips();
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    JobConf job = new JobConf(RemoveTipsAvro.RemoveTipsAvroReducer.class);
    RemoveTipsAvro.RemoveTipsAvroReducer reducer = new RemoveTipsAvro.RemoveTipsAvroReducer();
    RemoveTipsAvro stage= new RemoveTipsAvro();
    Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
    int tiplength= 4;
    definitions.get("tiplength").addToJobConf(job, new Integer(tiplength));
    reducer.configure(job);

    for (ReduceTestCaseData case_data : case_data_list) {

      AvroCollectorMock<GraphNodeData> collector_mock = new AvroCollectorMock<GraphNodeData>();
      try {
        CharSequence key = case_data.reducer_input_key;
        reducer.reduce(key, case_data.mapOutList, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
      assertReduceOutput(case_data, collector_mock);
    }
  }

  @Test
  public void testRun() {
    final int K = 3;
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGG", K);
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
    RemoveTipsAvro run_tips = new RemoveTipsAvro();
    File output_path = new File(temp, "output");
    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
       "--K=" + K, "--tiplength=" + 2 * K,
      };
    try {
      run_tips.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
