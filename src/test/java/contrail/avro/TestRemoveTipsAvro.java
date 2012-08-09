package contrail.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

    if (actual_message.value().getNode().getNodeId().toString().equals("ATC")) {
      System.out.println("Hello");
    }
    assertEquals(expected_message, actual_message);
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
    // TIPLENGTH is 4 for this case
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
      tip_msg.setEdgeStrands(StrandsForEdge.FR);
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
    int TIPLENGTH= 4;
    JobConf job = new JobConf(RemoveTipsAvro.RemoveTipsAvroMapper.class);
    definitions.get("tiplength").addToJobConf(job, new Integer(TIPLENGTH));
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
    List <RemoveTipMessage> map_out_list;
    GraphNodeData expected_node_data;
  }
  private void assertReduceOutput(
      ReduceTestCaseData case_data,
      AvroCollectorMock<GraphNodeData> collector_mock) {

    assertEquals(1, collector_mock.data.size());
    assertEquals(case_data.expected_node_data, collector_mock.data.get(0));
  }
  private List<ReduceTestCaseData> constructReduceTips() {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);
    graph.addEdge("ATC", "TCA", 2);

    List<ReduceTestCaseData> test_data_list= new ArrayList<ReduceTestCaseData> ();
    List <RemoveTipMessage> map_out_list= new ArrayList <RemoveTipMessage>();
    ReduceTestCaseData test_data= new ReduceTestCaseData();

    List <RemoveTipMessage> map_out_list2= new ArrayList <RemoveTipMessage>();
    ReduceTestCaseData test_data2= new ReduceTestCaseData();
    // Output of the Mapper is in the form {Key, Message <id,Strand>}; mapper outputs are as follows
    // nodeid(TCA), <TCA,null>
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCA"));
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      map_out_list.add(msg);
    }
    // nodeid(TCA), <ATC, FRStrand>
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("ATC"));
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      msg.setEdgeStrands(StrandsForEdge.FR);
      map_out_list.add(msg);
    }

    // both messages are now in a List; we now set the expected node/ key for those msg
    GraphNode temp= graph.getNode(graph.findNodeIdForSequence("TCA"));
    // update the node; edge will be removed in reducer
    temp.removeNeighbor(graph.getNode(graph.findNodeIdForSequence("ATC")).getNodeId());
    test_data.expected_node_data= temp.getData();
    test_data.map_out_list= map_out_list;

    // add key/msg list to Object List
    test_data_list.add(test_data);

    // nodeid(AAATC), <AAATC,null>
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("AAATC"));
      RemoveTipMessage msg = new RemoveTipMessage();
      msg.setNode(node.getData());
      map_out_list2.add(msg);
    }
    test_data2.expected_node_data= graph.getNode(graph.findNodeIdForSequence("AAATC")).getData();
    test_data2.map_out_list= map_out_list2;

    // add key/msg list to Object List
    test_data_list.add(test_data2);
    return test_data_list;
  }
  @Test
  public void testReduce() {
    List <ReduceTestCaseData> case_data_list= constructReduceTips();
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    JobConf job = new JobConf(RemoveTipsAvro.RemoveTipsAvroReducer.class);
    RemoveTipsAvro.RemoveTipsAvroReducer reducer = new RemoveTipsAvro.RemoveTipsAvroReducer();
    reducer.configure(job);

    for (ReduceTestCaseData case_data : case_data_list) {

      AvroCollectorMock<GraphNodeData> collector_mock = new AvroCollectorMock<GraphNodeData>();
      try {
        CharSequence key = case_data.expected_node_data.getNodeId();
        reducer.reduce(key, case_data.map_out_list, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
      assertReduceOutput(case_data, collector_mock);
    }
  }
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
    RemoveTipsAvro run_tips = new RemoveTipsAvro();
    File output_path = new File(temp, "output");
    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
      };
    try {
      run_tips.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
