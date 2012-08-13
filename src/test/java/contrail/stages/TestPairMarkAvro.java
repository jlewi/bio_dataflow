// Author: Jeremy Lewi
package contrail.stages;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

import contrail.ReporterMock;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.SimpleGraphBuilder;
import contrail.graph.TailData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

/** Extend PairMergeAvro so we can access the mapper and reducer.*/
public class TestPairMarkAvro extends PairMarkAvro {
  // A CoinFlipper which is not random but uses a hash table to map
  // strings to coin flips. This makes it easy to control the tosses assigned
  // to the nodes.
  private static class CoinFlipperFixed extends CoinFlipper {
    public HashMap<String, CoinFlip> tosses;
    public CoinFlipperFixed() {
      super(0);
      tosses = new HashMap<String, CoinFlip>();
    }

    public CoinFlip flip(String seed) {
      if (!tosses.containsKey(seed)) {
        throw new RuntimeException("Flipper is missing seed:" + seed);
      }
      return tosses.get(seed);
    }
  }

  // Returns true if the strand of the specified node is compressible.
  // This is used to setup some of the test cases.
  private boolean isCompressibleStrand(
      Map<String, GraphNode> nodes, String nodeid, DNAStrand strand) {
    GraphNode node = nodes.get(nodeid);

    if (node == null) {
      fail("Could not find node:" + nodeid);
    }

    TailData tail = node.getTail(strand, EdgeDirection.OUTGOING);
    if (tail == null) {
      return false;
    }

    // Check if the other node has a tail in this direction.
    GraphNode other_node = nodes.get(tail.terminal.nodeId);
    if (other_node == null) {
      fail("Could not find node:" + tail.terminal.nodeId);
    }
    TailData other_tail = other_node.getTail(
        tail.terminal.strand, EdgeDirection.INCOMING);

    if (other_tail == null) {
      return false;
    }

    // Sanity check. The terminal for the other_node should be this node.
    assertEquals(new EdgeTerminal(nodeid, strand), other_tail.terminal);

    return true;
  }

  // Determine which strands for the given node are compressible.
  private CompressibleStrands isCompressible(
      Map<String, GraphNode> nodes, String nodeid) {
    boolean f_compressible =
        isCompressibleStrand(nodes, nodeid, DNAStrand.FORWARD);
    boolean r_compressible =
        isCompressibleStrand(nodes, nodeid, DNAStrand.FORWARD);

    if (f_compressible && r_compressible) {
      return CompressibleStrands.BOTH;
    }

    if (f_compressible) {
      return CompressibleStrands.FORWARD;
    }

    if (r_compressible) {
      return CompressibleStrands.REVERSE;
    }

    return CompressibleStrands.NONE;
  }

  // Class to contain the input and output pairs for the mapper.
  private static class MapperInputOutput {
    public MapperInputOutput () {
      edge_updates = new HashMap<String, EdgeUpdateForMerge>();
    }
    CompressibleNodeData input_node;
    HashMap<String, EdgeUpdateForMerge> edge_updates;
    NodeInfoForMerge output_node;
  }

  // This class serves as a container for the data for testing the mapper.
  private static class MapperTestCase {
    public MapperTestCase() {
      inputs_outputs = new HashMap<String, MapperInputOutput>();
    }
    // The various input/output sets for this test case.
    // The key is typically the id of the node processed as input.
    public HashMap<String, MapperInputOutput> inputs_outputs;

    // The flipper to use in the test.
    public CoinFlipper flipper;
  }

  private MapperTestCase simpleMapperTest() {
    // Construct the simplest mapper test case.
    // We have three nodes assigned the states Down, Up, Down.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("AACTG", 3);

    MapperTestCase test_case = new MapperTestCase();
    for (GraphNode node: builder.getAllNodes().values()) {
      CompressibleNodeData data = new CompressibleNodeData();
      data.setNode(node.getData());
      data.setCompressibleStrands(isCompressible(
          builder.getAllNodes(), node.getNodeId()));

      MapperInputOutput input_output = new MapperInputOutput();
      input_output.input_node = data;

      // Each node outputs at least itself.
      NodeInfoForMerge node_info_for_merge = new NodeInfoForMerge();
      node_info_for_merge.setCompressibleNode(
          CompressUtil.copyCompressibleNode(data));

      if (node.getNodeId().toString().equals(
          builder.findNodeIdForSequence("ACT"))) {
        node_info_for_merge.setStrandToMerge(CompressibleStrands.FORWARD);
      } else {
        node_info_for_merge.setStrandToMerge(CompressibleStrands.NONE);
      }

      input_output.output_node = node_info_for_merge;
      test_case.inputs_outputs.put(node.getNodeId(), input_output);
    }

    CoinFlipperFixed flipper = new CoinFlipperFixed();
    test_case.flipper = flipper;
    flipper.tosses.put(
        builder.findNodeIdForSequence("AAC"), CoinFlipper.CoinFlip.DOWN);
    flipper.tosses.put(
        builder.findNodeIdForSequence("ACT"), CoinFlipper.CoinFlip.UP);
    flipper.tosses.put(
        builder.findNodeIdForSequence("CTG"), CoinFlipper.CoinFlip.DOWN);

    // For the middle node we need to add the edge update to the output.
    String up_id = builder.findNodeIdForSequence("ACT");
    GraphNode up_node = builder.getNode(up_id);

    // The up node gets merged along the forward strand.
    EdgeTerminal new_terminal = up_node.getEdgeTerminals(
        DNAStrand.FORWARD, EdgeDirection.OUTGOING).get(0);

    EdgeTerminal update_target = up_node.getEdgeTerminals(
        DNAStrand.FORWARD, EdgeDirection.INCOMING).get(0);

    EdgeUpdateForMerge edge_update = new EdgeUpdateForMerge();
    edge_update.setOldId(up_id);
    edge_update.setOldStrand(DNAStrand.FORWARD);
    edge_update.setNewId(new_terminal.nodeId);
    edge_update.setNewStrand(new_terminal.strand);

    MapperInputOutput input_output = test_case.inputs_outputs.get(up_id);
    input_output.edge_updates.put(update_target.nodeId, edge_update);
    return test_case;
  }

  private MapperTestCase mapperNoMergeTest() {
    // Construct the test case where the nodes can't be compressed.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTG", 3);
    builder.addEdge("ACT","CTC", 2);

    MapperTestCase test_case = new MapperTestCase();
    for (GraphNode node: builder.getAllNodes().values()) {
      CompressibleNodeData data = new CompressibleNodeData();
      data.setNode(node.getData());

      // Nodes aren't compressible.
      data.setCompressibleStrands(CompressibleStrands.NONE);

      MapperInputOutput input_output = new MapperInputOutput();
      input_output.input_node = CompressUtil.copyCompressibleNode(data);

      // Each node outputs at itself.
      NodeInfoForMerge node_info_for_merge = new NodeInfoForMerge();
      node_info_for_merge.setCompressibleNode(
          CompressUtil.copyCompressibleNode(data));
      node_info_for_merge.setStrandToMerge(CompressibleStrands.NONE);
      input_output.output_node = node_info_for_merge;
      test_case.inputs_outputs.put(node.getNodeId(), input_output);
    }

    // Use the random coin flipper.
    test_case.flipper = new CoinFlipper(12);
    return test_case;
  }

  private MapperTestCase mapperConvertDownToUpTest() {
    // Construct a chain of 3 nodes all assigned down.
    // Check that we properly convert the node in the middle to up.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();

    // Add the three nodes. We need to assign the node in the middle
    // a node id less than the two terminals.
    builder.addNode("node_1", "ACT");
    builder.addNode("node_0", "CTG");
    builder.addNode("node_2", "TGA");

    builder.addEdge("ACT", "CTG", 2);
    builder.addEdge("CTG", "TGA", 2);

    String middle_id = builder.findNodeIdForSequence("CTG");

    MapperTestCase test_case = new MapperTestCase();

    CoinFlipperFixed flipper = new CoinFlipperFixed();
    test_case.flipper = flipper;
    for (GraphNode node: builder.getAllNodes().values()) {
      CompressibleNodeData data = new CompressibleNodeData();
      data.setNode(node.getData());

      // Nodes are all compressible.
      data.setCompressibleStrands(
          isCompressible(builder.getAllNodes(), node.getNodeId()));

      MapperInputOutput input_output = new MapperInputOutput();
      test_case.inputs_outputs.put(node.getNodeId(), input_output);
      input_output.input_node = data;

      NodeInfoForMerge output = new NodeInfoForMerge();
      output.setCompressibleNode(CompressUtil.copyCompressibleNode(data));
      input_output.output_node = output;

      if (node.getNodeId().equals(middle_id)) {
        output.setStrandToMerge(CompressibleStrands.FORWARD);
      } else {
        output.setStrandToMerge(CompressibleStrands.NONE);
      }

      if (node.getNodeId().equals(middle_id)) {
        EdgeUpdateForMerge edge_update = new EdgeUpdateForMerge();
        edge_update.setOldId(node.getNodeId());
        edge_update.setOldStrand(DNAStrand.FORWARD);

        EdgeTerminal new_terminal = node.getEdgeTerminals(
            DNAStrand.FORWARD, EdgeDirection.OUTGOING).get(0);
        edge_update.setNewId(new_terminal.nodeId);
        edge_update.setNewStrand(new_terminal.strand);

        EdgeTerminal update_terminal = node.getEdgeTerminals(
            DNAStrand.FORWARD, EdgeDirection.INCOMING).get(0);

        input_output.edge_updates.put(update_terminal.nodeId, edge_update);
      }
      // All nodes assigned down.
      flipper.tosses.put(
          node.getNodeId(), CoinFlipper.CoinFlip.DOWN);
    }
    return test_case;
  }

  // Check the output of the mapper matches the expected result.
  private void assertMapperOutput(
      MapperInputOutput input_output,
      AvroCollectorMock<Pair<CharSequence, PairMarkOutput>>
          collector_mock) {
    // The mapper should output the edge updates and the node.
    int num_expected_outputs = input_output.edge_updates.size() + 1;
    assertEquals(num_expected_outputs, collector_mock.data.size());

    String input_id = input_output.input_node.getNode().getNodeId().toString();
    HashMap<String, EdgeUpdateForMerge> edge_updates =
        new HashMap<String, EdgeUpdateForMerge>();
    // Separate the output into edge updates and the node itself.
    int num_nodes = 0;
    for (Pair<CharSequence, PairMarkOutput> out_pair: collector_mock.data) {
      if (out_pair.value().getPayload() instanceof NodeInfoForMerge) {
        ++num_nodes;
        // The node should be sent to itself.
        assertEquals(input_id, out_pair.key().toString());
        NodeInfoForMerge node_info =
            (NodeInfoForMerge) out_pair.value().getPayload();
        assertEquals(input_output.output_node, node_info);
      } else {
        edge_updates.put(
            out_pair.key().toString(),
            (EdgeUpdateForMerge) out_pair.value().getPayload());
      }
    }
    assertEquals(1, num_nodes);
    assertEquals(input_output.edge_updates, edge_updates);
  }

  @Test
  public void testMapper() {
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();
    test_cases.add(mapperNoMergeTest());
    test_cases.add(simpleMapperTest());
    test_cases.add(mapperConvertDownToUpTest());

    PairMarkMapper mapper = new PairMarkMapper();

    JobConf job = new JobConf(PairMarkMapper.class);
    PairMarkAvro stage = new PairMarkAvro();
    Map<String, ParameterDefinition> parameters =
        stage.getParameterDefinitions();
    parameters.get("randseed").addToJobConf(job, new Long(1));

    mapper.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapperTestCase test_case: test_cases) {
      for (MapperInputOutput input_output: test_case.inputs_outputs.values()) {
        // We need a new collector for each invocation because the
        // collector stores the outputs of the mapper.
        AvroCollectorMock<Pair<CharSequence, PairMarkOutput>>
          collector_mock =
          new AvroCollectorMock<Pair<CharSequence, PairMarkOutput>>();

        mapper.setFlipper(test_case.flipper);
        try {
          mapper.map(
              input_output.input_node, collector_mock, reporter);
        }
        catch (IOException exception){
          fail("IOException occured in map: " + exception.getMessage());
        }

        assertMapperOutput(input_output, collector_mock);
      }
    }
  }

  // A container class used for organizing the data for the reducer tests.
  private class ReducerTestCase {
    public String reducer_key;
    // The input to the reducer.
    public List<PairMarkOutput> input;
    // The expected output from the reducer.
    public NodeInfoForMerge expected_output;
  }

  // Asserts that the output of the reducer is correct for this test case.
  private void assertReducerTestCase(
      ReducerTestCase test_case,
      AvroCollectorMock<NodeInfoForMerge> collector_mock) {

    assertEquals(1, collector_mock.data.size());
    NodeInfoForMerge output = collector_mock.data.get(0);

    // Check the nodes are equal.
    assertEquals(test_case.expected_output, output);
  }

  private ReducerTestCase reducerNoUpdatesTest() {
    // Construct a simple reduce test case in which no edges are updated.
    ReducerTestCase test_case = new ReducerTestCase();

    test_case.input = new ArrayList<PairMarkOutput>();
    GraphNode node = new GraphNode();
    node.setNodeId("somenode");
    Sequence sequence = new Sequence("ACGCT", DNAAlphabetFactory.create());
    node.setSequence(sequence);
    test_case.reducer_key = node.getNodeId();

    CompressibleNodeData merge_data = new CompressibleNodeData();
    merge_data.setNode(node.clone().getData());
    merge_data.setCompressibleStrands(CompressibleStrands.NONE);

    NodeInfoForMerge out_node = new NodeInfoForMerge();
    out_node.setCompressibleNode(
        CompressUtil.copyCompressibleNode(merge_data));
    out_node.setStrandToMerge(CompressibleStrands.NONE);

    PairMarkOutput mapper_output = new PairMarkOutput();
    mapper_output.setPayload(out_node);
    test_case.input.add(mapper_output);

    test_case.expected_output = new NodeInfoForMerge();
    test_case.expected_output.setCompressibleNode(
        CompressUtil.copyCompressibleNode(merge_data));    ;
    test_case.expected_output.setStrandToMerge(CompressibleStrands.NONE);

    return test_case;
  }

  private ReducerTestCase reducerUpdateTest() {
    // Construct a simple reduce test case in which one edge is connected
    // to a node which gets merged. So the edge needs to be moved.
    ReducerTestCase test_case = new ReducerTestCase();

    test_case.input = new ArrayList<PairMarkOutput>();
    GraphNode node = new GraphNode();
    GraphNode output_node = null;
    node.setNodeId("somenode");
    Sequence sequence = new Sequence("ACG", DNAAlphabetFactory.create());
    node.setSequence(sequence);

    // Add two outgoing edges.
    EdgeTerminal terminal1 = new EdgeTerminal("node1", DNAStrand.FORWARD);
    EdgeTerminal terminal2 = new EdgeTerminal("node2", DNAStrand.REVERSE);
    node.addOutgoingEdge(DNAStrand.FORWARD, terminal1);

    // Clone the node before adding the edge that will be moved.
    output_node = node.clone();
    node.addOutgoingEdge(DNAStrand.FORWARD, terminal2);

    EdgeUpdateForMerge edge_update = new EdgeUpdateForMerge();
    edge_update.setOldId("node2");
    edge_update.setOldStrand(DNAStrand.REVERSE);
    edge_update.setNewId("node3");
    edge_update.setNewStrand(DNAStrand.FORWARD);

    EdgeTerminal terminal3 = new EdgeTerminal("node3", DNAStrand.FORWARD);
    output_node.addOutgoingEdge(DNAStrand.FORWARD, terminal3);
    test_case.reducer_key = node.getNodeId();

    CompressibleNodeData input_data = new CompressibleNodeData();
    input_data.setNode(node.getData());
    input_data.setCompressibleStrands(CompressibleStrands.REVERSE);
    NodeInfoForMerge input_node = new NodeInfoForMerge();
    input_node.setCompressibleNode(input_data);
    input_node.setStrandToMerge(CompressibleStrands.REVERSE);

    PairMarkOutput map_output_node = new PairMarkOutput();
    map_output_node.setPayload(input_node);

    PairMarkOutput map_output_update = new PairMarkOutput();
    map_output_update.setPayload(edge_update);

    test_case.input.add(map_output_node);
    test_case.input.add(map_output_update);

    test_case.expected_output = new NodeInfoForMerge();
    test_case.expected_output.setCompressibleNode(
        new CompressibleNodeData());
    test_case.expected_output.getCompressibleNode().setNode(
        output_node.getData());
    test_case.expected_output.getCompressibleNode().setCompressibleStrands(
        CompressibleStrands.REVERSE);
    test_case.expected_output.setStrandToMerge(CompressibleStrands.REVERSE);

    return test_case;
  }

  @Test
  public void testReducer() {
    ArrayList<ReducerTestCase> test_cases = new ArrayList<ReducerTestCase>();
    test_cases.add(reducerNoUpdatesTest());
    test_cases.add(reducerUpdateTest());
    PairMarkReducer reducer = new PairMarkReducer();

    JobConf job = new JobConf(PairMarkReducer.class);

    reducer.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (ReducerTestCase test_case: test_cases) {
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<NodeInfoForMerge> collector_mock =
        new AvroCollectorMock<NodeInfoForMerge>();

      try {
        reducer.reduce(
            test_case.reducer_key, test_case.input, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertReducerTestCase(test_case, collector_mock);
    }
  }

  @Test
  public void testRun() {
    // This function tests that we can run the MR job without errors.
    // It doesn't test for correctness.
    MapperTestCase test_case = this.mapperConvertDownToUpTest();

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

    File avro_file = new File(temp, "compressible.avro");

    // Write the data to the file.
    Schema schema = (new CompressibleNodeData()).getSchema();
    DatumWriter<CompressibleNodeData> datum_writer =
        new SpecificDatumWriter<CompressibleNodeData>(schema);
    DataFileWriter<CompressibleNodeData> writer =
        new DataFileWriter<CompressibleNodeData>(datum_writer);

    try {
      writer.create(schema, avro_file);
      for (MapperInputOutput input_output: test_case.inputs_outputs.values()) {
        writer.append(input_output.input_node);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }

    // Run it.
    PairMarkAvro pair_merge = new PairMarkAvro();
    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
       "--randseed=12"};

    try {
      pair_merge.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
