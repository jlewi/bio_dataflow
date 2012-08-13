package contrail.stages;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

public class TestCompressibleAvro {

  /*
   * Check the output of the map is correct.
   */
  private void assertMapperOutput(
      GraphNodeData expected_node, HashMap<String, CompressibleMessage>
      expected_messages,
      AvroCollectorMock<Pair<CharSequence, CompressibleMapOutput>>
          collector_mock) {

    // Check the output.
    Iterator<Pair<CharSequence, CompressibleMapOutput>> it =
        collector_mock.data.iterator();

    boolean has_node = false;
    HashSet<String> has_messages = new HashSet<String>();

    while (it.hasNext()) {
      Pair<CharSequence, CompressibleMapOutput> pair = it.next();
      String key = pair.key().toString();

      if (key.equals(expected_node.getNodeId().toString())) {
        assertEquals(expected_node, pair.value().getNode());
        assertEquals("Message should not be set", null,
                     pair.value().getMessage());
        has_node = true;
        continue;
      }
      assertTrue("Message key is invalid", expected_messages.containsKey(key));
      assertEquals(expected_messages.get(key), pair.value().getMessage());
      assertEquals("Node should not be set.", null, pair.value().getNode());
      has_messages.add(key);
    }

    // Check we have the node.
    assertTrue(has_node);

    // Check we have all the expected messages.
    assertEquals(expected_messages.keySet(), has_messages);
  }

  // Store the data for a particular test case for the map phase.
  private static class MapTestCaseData {
    public GraphNodeData node;
    public HashMap<String, CompressibleMessage> expected_messages;
  }

  private MapTestCaseData constructMapLinearTestCase() {
    // Construct a linear graph and make sure we output messages
    // containing the nodes and messages to the neighbors.
    String main_chain = "ATCGC";
    int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString(main_chain, K);

    // Construct the list of expected messages. This is a hashtable
    // where the key is the id of the destination node.
    HashMap<String, CompressibleMessage> expected_messages =
        new HashMap<String, CompressibleMessage>();

    GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCG"));

    for (DNAStrand strand: DNAStrand.values()) {
      for (EdgeTerminal terminal :
           node.getEdgeTerminals(strand, EdgeDirection.OUTGOING)){
        CompressibleMessage message = new CompressibleMessage();
        message.setFromNodeId(node.getNodeId());
        StrandsForEdge strands = StrandsUtil.form(strand, terminal.strand);
        message.setStrands(strands);
        expected_messages.put(terminal.nodeId, message);
      }
    }

    MapTestCaseData case_data = new MapTestCaseData();
    case_data.node = node.getData();
    case_data.expected_messages = expected_messages;
    return case_data;
  }

  private MapTestCaseData constructMapLinearTestBranching() {
    // Construct a node which has indegree 1 and outdegree 2.
    String main_chain = "CATCG";
    int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString(main_chain, K);

    // Add another outgoing edge.
    graph.addEdge("ATC", "TCA", 2);

    // Construct the list of expected messages. This is a hashtable
    // where the key is the id of the destination node.
    HashMap<String, CompressibleMessage> expected_messages =
        new HashMap<String, CompressibleMessage>();

   GraphNode node = graph.getNode(graph.findNodeIdForSequence("ATC"));

     // We only send a single message.
    {
      CompressibleMessage message = new CompressibleMessage();
      message.setFromNodeId(node.getNodeId());

      // Edge is CAT ->ATC
      // Since we only send outgoing edges the message is
      // GAT -> ATG : FR
      message.setStrands(StrandsForEdge.RF);
      expected_messages.put(graph.findNodeIdForSequence("CAT"), message);
    }

    MapTestCaseData case_data = new MapTestCaseData();
    case_data.node = node.getData();
    case_data.expected_messages = expected_messages;
    return case_data;
  }

  @Test
  public void testMap() {
    // Test the mapper. We want to run all the different
    // cases using the same mapper instance as this is more likely to catch
    // issues with static values not being cleared between invocations.
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    CompressibleAvro.CompressibleMapper mapper =
        new CompressibleAvro.CompressibleMapper();

    JobConf job = new JobConf(CompressibleAvro.CompressibleMapper.class);

    mapper.configure(job);

    // Construct the different test cases.
    ArrayList<MapTestCaseData> test_cases = new ArrayList<MapTestCaseData>();
    test_cases.add(constructMapLinearTestCase());
    test_cases.add(constructMapLinearTestBranching());

    for (MapTestCaseData case_data : test_cases) {
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<CharSequence, CompressibleMapOutput>>
      collector_mock =
        new AvroCollectorMock<Pair<CharSequence, CompressibleMapOutput>>();
      try {
        mapper.map(
            case_data.node,
            collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertMapperOutput(
          case_data.node, case_data.expected_messages, collector_mock);
    }
  }

  private static class ReduceTestCaseData {
    // Store data used for a reduce test case.
    List<CompressibleMapOutput> map_outputs;

    // The expected annotated node output
    CompressibleNodeData expected_annotated_node;
  }

  private void assertReduceOutput(
      ReduceTestCaseData case_data,
      AvroCollectorMock<CompressibleNodeData> collector_mock) {

    // Reducer should produce a single output.
    assertEquals(1, collector_mock.data.size());

    assertEquals(
        case_data.expected_annotated_node, collector_mock.data.get(0));
  }

  private ReduceTestCaseData constructReduceLinearTestCase() {
    // Construct the graph A->B->C. We construct the messages
    // for B which should be compressible in both directions.
    String main_chain = "ATCGC";
    int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString(main_chain, K);

    GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCG"));

    // Construct the list of expected messages. This is a hashtable
    // where the key is the id of the destination node.
    List<CompressibleMapOutput> map_outputs =
        new ArrayList<CompressibleMapOutput>();

    {
      CompressibleMapOutput output = new CompressibleMapOutput();
      // Clone the data so that the message has its own data.
      output.setNode(node.clone().getData());
      map_outputs.add(output);
    }
    {
      CompressibleMapOutput output = new CompressibleMapOutput();
      CompressibleMessage message = new CompressibleMessage();

      // We always deal with outgoing edges.
      // TCG->CGC implies GCG -> CGA : RF
      message.setFromNodeId(graph.findNodeIdForSequence("CGC"));
      message.setStrands(StrandsForEdge.RF);

      output.setMessage(message);
      map_outputs.add(output);
    }
    {
      CompressibleMapOutput output = new CompressibleMapOutput();
      CompressibleMessage message = new CompressibleMessage();

      // We always deal with the forward strand.
      message.setFromNodeId(graph.findNodeIdForSequence("ATC"));
      message.setStrands(StrandsForEdge.FR);
      output.setMessage(message);
      map_outputs.add(output);
    }

    ReduceTestCaseData case_data = new ReduceTestCaseData();
    case_data.map_outputs = map_outputs;

    CompressibleNodeData annotated_node = new CompressibleNodeData();

    annotated_node.setNode(node.clone().getData());
    annotated_node.setCompressibleStrands(CompressibleStrands.BOTH);
    case_data.expected_annotated_node = annotated_node;
    return case_data;
  }

  @Test
  public void testReduce() {
    // We test all the reduce cases using a single instance of the reducer
    // class to make sure cached values are properly cleared.

    ArrayList<ReduceTestCaseData> test_cases =
        new ArrayList<ReduceTestCaseData> ();

    test_cases.add(constructReduceLinearTestCase());

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    JobConf job = new JobConf(CompressibleAvro.CompressibleReducer.class);

    CompressibleAvro.CompressibleReducer reducer =
        new CompressibleAvro.CompressibleReducer();

    reducer.configure(job);

    for (ReduceTestCaseData case_data: test_cases) {
      // We need a new collector for each reduce invocation.
      AvroCollectorMock<CompressibleNodeData> collector_mock =
          new AvroCollectorMock<CompressibleNodeData>();

      try {
        CharSequence key =
            case_data.expected_annotated_node.getNode().getNodeId();
        reducer.reduce(key,
            case_data.map_outputs, collector_mock, reporter);
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
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
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
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }

    // Run it.
    CompressibleAvro compressible = new CompressibleAvro();
    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString()};

    try {
      compressible.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
