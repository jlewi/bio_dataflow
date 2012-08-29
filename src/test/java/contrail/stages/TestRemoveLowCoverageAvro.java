package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import contrail.RemoveNeighborMessage;
import contrail.ReporterMock;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;

public class TestRemoveLowCoverageAvro extends RemoveLowCoverageAvro  {

  /*
   * Check the output of the map is correct.
   */
  private void assertMapperOutput(
      GraphNodeData expected_node, HashMap<String, RemoveNeighborMessage> expected_messages,
      AvroCollectorMock<Pair<CharSequence, RemoveNeighborMessage>> collector_mock) {

    Iterator<Pair<CharSequence, RemoveNeighborMessage>> it = collector_mock.data.iterator();

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

  private List<MapTestCaseData> constructMapCases() {
    List <MapTestCaseData> cases= new ArrayList<MapTestCaseData>();
    MapTestCaseData low_cov = constructLowCoverageNode();
    MapTestCaseData high_cov = constructNonLowCoverageNode();
    cases.add(high_cov);
    cases.add(low_cov);
    return cases;
  }

  @Test
  public void testMap() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    RemoveLowCoverageAvro.RemoveLowCoverageAvroMapper mapper =
        new RemoveLowCoverageAvro.RemoveLowCoverageAvroMapper();
    RemoveLowCoverageAvro stage= new RemoveLowCoverageAvro();
    Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();

    JobConf job = new JobConf(RemoveLowCoverageAvro.RemoveLowCoverageAvroMapper.class);

    definitions.get("length_thresh").addToJobConf(job, new Integer(5));
    definitions.get("low_cov_thresh").addToJobConf(job, new Float(4));
    mapper.configure(job);

    // Construct the different test cases.
    List <MapTestCaseData> test_cases = constructMapCases();
    for (MapTestCaseData case_data : test_cases) {
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
    List <RemoveNeighborMessage> map_out_list;
    GraphNodeData expected_node_data;
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

  private List<ReduceTestCaseData> constructReduceData() {
      List<ReduceTestCaseData> test_data_list= new ArrayList<ReduceTestCaseData> ();
      ReduceTestCaseData high_cov = constructHighCoverageData();
      ReduceTestCaseData low_cov = constructLowCoverageData();
      test_data_list.add(low_cov);
      test_data_list.add(high_cov);
      test_data_list.add(reducerNodeAlreadyRemoved());
      return test_data_list;
  }

  @Test
  public void testReduce() {
    List <ReduceTestCaseData> case_data_list= constructReduceData();
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    JobConf job = new JobConf(RemoveLowCoverageAvro.RemoveLowCoverageAvroReducer.class);
    RemoveLowCoverageAvro.RemoveLowCoverageAvroReducer reducer =
        new RemoveLowCoverageAvro.RemoveLowCoverageAvroReducer();
    reducer.configure(job);
    for (ReduceTestCaseData case_data : case_data_list) {

      AvroCollectorMock<GraphNodeData> collector_mock = new AvroCollectorMock<GraphNodeData>();
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