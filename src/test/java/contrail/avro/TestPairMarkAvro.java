package contrail.avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.Pair;
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

// Extend PairMergeAvro so we can access the mapper and reducer.
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

  // Return true if the strand of the specified node is compressible.
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

  // Class to contain the input and output pairs used for the mapper.
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
        builder.findNodeIdForSequence("AAC"), CoinFlipper.CoinFlip.Down);
    flipper.tosses.put(
        builder.findNodeIdForSequence("ACT"), CoinFlipper.CoinFlip.Up);
    flipper.tosses.put(
        builder.findNodeIdForSequence("CTG"), CoinFlipper.CoinFlip.Down);

    // For the middle node we need to add the edge update.
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
          node.getNodeId(), CoinFlipper.CoinFlip.Down);
    }
    return test_case;
  }

  // Check the output of the mapper matches the expected result.
  private void assertMapperOutput(
      MapperInputOutput input_output,
      AvroCollectorMock<Pair<CharSequence, PairMarkOutput>>
          collector_mock) {
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
    job.setLong("randseed", 11);

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
//
//  private ReducerTestCase reducerSimpleMergeTest() {
//    // Construct a simple reduce test case in which two nodes are merged.
//    // TODO(jlewi): We should really randomize this so we cover more cases.
//    ReducerTestCase test_case = new ReducerTestCase();
//    test_case.K = 3;
//
//    SimpleGraphBuilder builder = new SimpleGraphBuilder();
//    // We will merge node ACT and CTT
//    builder.addKMersForString("ACTT", test_case.K);
//
//    // Add some incoming/outgoing edges so that we have edges that need to
//    // be updated.
//    builder.addEdge("CAC", "ACT", test_case.K - 1);
//    builder.addEdge("GAC", "ACT", test_case.K - 1);
//    builder.addEdge("CTT", "TTA", test_case.K - 1);
//
//    test_case.input = new ArrayList<CompressibleNodeData>();
//    {
//      GraphNode node = builder.getNode(builder.findNodeIdForSequence("ACT"));
//      CompressibleNodeData merge_data = new CompressibleNodeData();
//      merge_data.setCompressibleStrands(CompressibleStrands.FORWARD);
//      merge_data.setNode(node.clone().getData());
//      test_case.input.add(merge_data);
//    }
//    {
//      GraphNode node = builder.getNode(builder.findNodeIdForSequence("CTT"));
//      CompressibleNodeData merge_data = new CompressibleNodeData();
//      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
//      merge_data.setNode(node.clone().getData());
//      test_case.input.add(merge_data);
//    }
//
//    // Construct the expected output.
//    // The sequence ACTT is the reverse strand.
//    GraphNode merged_node = new GraphNode();
//    Sequence merged_sequence =
//        new Sequence("ACTT", DNAAlphabetFactory.create());
//    merged_node.setCanonicalSequence(DNAUtil.canonicalseq(merged_sequence));
//    merged_node.addIncomingEdge(
//        DNAStrand.REVERSE, new EdgeTerminal("CAC", DNAStrand.FORWARD));
//    merged_node.addIncomingEdge(
//        DNAStrand.REVERSE, new EdgeTerminal("GAC", DNAStrand.FORWARD));
//
//    merged_node.addOutgoingEdge(
//        DNAStrand.REVERSE, new EdgeTerminal("TAA", DNAStrand.REVERSE));
//    merged_node.setNodeId(builder.findNodeIdForSequence("CTT"));
//
//    test_case.reducer_key = merged_node.getNodeId();
//
//    CompressibleNodeData node_data = new CompressibleNodeData();
//    node_data.setNode(merged_node.clone().getData());
//    node_data.setCompressibleStrands(CompressibleStrands.REVERSE);
//    test_case.expected_output = new PairMergeOutput();
//    test_case.expected_output.setCompressibleNode(node_data);
//    test_case.expected_output.setUpdateMessages(
//        new ArrayList<EdgeUpdateAfterMerge>());
//
//    // Add the messages
//    {
//     EdgeUpdateAfterMerge update = new EdgeUpdateAfterMerge();
//     update.setNodeToUpdate(builder.findNodeIdForSequence("GAC"));
//     update.setOldTerminalId(builder.findNodeIdForSequence("ACT"));
//     update.setNewTerminalId(merged_node.getNodeId());
//     update.setOldStrands(StrandsForEdge.FF);
//     update.setNewStrands(StrandsForEdge.FR);
//
//     test_case.expected_output.getUpdateMessages().add(update);
//    }
//
//    {
//      EdgeUpdateAfterMerge update = new EdgeUpdateAfterMerge();
//      update.setNodeToUpdate(builder.findNodeIdForSequence("CAC"));
//      update.setOldTerminalId(builder.findNodeIdForSequence("ACT"));
//      update.setNewTerminalId(merged_node.getNodeId());
//      update.setOldStrands(StrandsForEdge.FF);
//      update.setNewStrands(StrandsForEdge.FR);
//
//      test_case.expected_output.getUpdateMessages().add(update);
//    }
//
//    {
//      EdgeUpdateAfterMerge update = new EdgeUpdateAfterMerge();
//      update.setNodeToUpdate(builder.findNodeIdForSequence("TTA"));
//      update.setOldTerminalId(builder.findNodeIdForSequence("CTT"));
//      update.setNewTerminalId(merged_node.getNodeId());
//
//      // The old edge is RC(CTT->TTA) TAA->AAG (FF)
//      // The merged sequence is AAGT = RC(ACTT)
//      update.setOldStrands(StrandsForEdge.FF);
//      update.setNewStrands(StrandsForEdge.FF);
//
//      test_case.expected_output.getUpdateMessages().add(update);
//     }
//
//    return test_case;
//  }
//
//  private ReducerTestCase reducerTwoMergeTest() {
//    // Construct a test case where two nodes are merged into another node.
//    // i.e we have the chain A->B-C, and nodes A,C get sent to B to be
//    // merged.
//    ReducerTestCase test_case = new ReducerTestCase();
//    test_case.K = 3;
//
//    SimpleGraphBuilder builder = new SimpleGraphBuilder();
//    // AAT->ATC>TCT
//    builder.addKMersForString("AATCT", test_case.K);
//
//    // Add some incoming/outgoing edges so that we have edges that need to
//    // be updated.
//    builder.addEdge("TAA", "AAT", test_case.K - 1);
//    builder.addEdge("TCT", "CTT", test_case.K - 1);
//
//    test_case.input = new ArrayList<CompressibleNodeData>();
//    {
//      GraphNode node = builder.getNode(builder.findNodeIdForSequence("AAT"));
//      CompressibleNodeData merge_data = new CompressibleNodeData();
//      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
//      merge_data.setNode(node.clone().getData());
//      test_case.input.add(merge_data);
//    }
//    {
//      GraphNode node = builder.getNode(builder.findNodeIdForSequence("ATC"));
//      CompressibleNodeData merge_data = new CompressibleNodeData();
//      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
//      merge_data.setNode(node.clone().getData());
//      test_case.input.add(merge_data);
//    }
//    {
//      GraphNode node = builder.getNode(builder.findNodeIdForSequence("TCT"));
//      CompressibleNodeData merge_data = new CompressibleNodeData();
//      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
//      merge_data.setNode(node.clone().getData());
//      test_case.input.add(merge_data);
//    }
//    // Construct the expected output.
//    GraphNode merged_node = new GraphNode();
//    Sequence merged_sequence =
//        new Sequence("AATCT", DNAAlphabetFactory.create());
//    merged_node.setCanonicalSequence(DNAUtil.canonicalseq(merged_sequence));
//    merged_node.addIncomingEdge(
//        DNAStrand.FORWARD, new EdgeTerminal("TAA", DNAStrand.FORWARD));
//    merged_node.addOutgoingEdge(
//        DNAStrand.FORWARD, new EdgeTerminal("AAG", DNAStrand.REVERSE));
//    merged_node.setNodeId(builder.findNodeIdForSequence("ATC"));
//
//    test_case.reducer_key = merged_node.getNodeId();
//
//    CompressibleNodeData node_output = new CompressibleNodeData();
//    node_output.setCompressibleStrands(CompressibleStrands.BOTH);
//    node_output.setNode(merged_node.clone().getData());
//    test_case.expected_output = new PairMergeOutput();
//    test_case.expected_output.setCompressibleNode(node_output);
//    test_case.expected_output.setUpdateMessages(
//        new ArrayList<EdgeUpdateAfterMerge>());
//
//    // Add the messages
//    {
//     // Old Edge: TAA ->AAT
//     // New Edge: TAA ->AATCT
//     EdgeUpdateAfterMerge update = new EdgeUpdateAfterMerge();
//     update.setNodeToUpdate(builder.findNodeIdForSequence("TAA"));
//     update.setOldTerminalId(builder.findNodeIdForSequence("AAT"));
//     update.setNewTerminalId(merged_node.getNodeId());
//     update.setOldStrands(StrandsForEdge.FF);
//     update.setNewStrands(StrandsForEdge.FF);
//
//     test_case.expected_output.getUpdateMessages().add(update);
//    }
//
//    {
//      // Old Edge: AAG -> AGA
//      // New Edge: AAG -> AGATT
//      EdgeUpdateAfterMerge update = new EdgeUpdateAfterMerge();
//      update.setNodeToUpdate(builder.findNodeIdForSequence("AAG"));
//      update.setOldTerminalId(builder.findNodeIdForSequence("AGA"));
//      update.setNewTerminalId(merged_node.getNodeId());
//      update.setOldStrands(StrandsForEdge.FF);
//      update.setNewStrands(StrandsForEdge.FR);
//
//      test_case.expected_output.getUpdateMessages().add(update);
//     }
//
//    return test_case;
//  }
//
  @Test
  public void testReducer() {
    ArrayList<ReducerTestCase> test_cases = new ArrayList<ReducerTestCase>();
    test_cases.add(reducerNoUpdatesTest());
    //test_cases.add(reducerSimpleMergeTest());
    //test_cases.add(reducerTwoMergeTest());
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
//
//  @Test
//  public void testRun() {
//    // This function tests that we can run the job without errors.
//    // It doesn't test for correctness.
//    MapperTestCase test_case = this.mapperConvertDownToUpTest();
//
//    File temp = null;
//
//    try {
//      temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
//    } catch (IOException exception) {
//      fail("Could not create temporary file. Exception:" +
//          exception.getMessage());
//    }
//    if(!(temp.delete())){
//        throw new RuntimeException(
//            "Could not delete temp file: " + temp.getAbsolutePath());
//    }
//
//    if(!(temp.mkdir())) {
//        throw new RuntimeException(
//            "Could not create temp directory: " + temp.getAbsolutePath());
//    }
//
//    File avro_file = new File(temp, "compressible.avro");
//
//    // Write the data to the file.
//    Schema schema = (new CompressibleNodeData()).getSchema();
//    DatumWriter<CompressibleNodeData> datum_writer =
//        new SpecificDatumWriter<CompressibleNodeData>(schema);
//    DataFileWriter<CompressibleNodeData> writer =
//        new DataFileWriter<CompressibleNodeData>(datum_writer);
//
//    try {
//      writer.create(schema, avro_file);
//      for (CompressibleNodeData node: test_case.input) {
//        writer.append(node);
//      }
//      writer.close();
//    } catch (IOException exception) {
//      fail("There was a problem writing the graph to an avro file. Exception:" +
//          exception.getMessage());
//    }
//
//    // Run it.
//    PairMergeAvro pair_merge = new PairMergeAvro();
//    File output_path = new File(temp, "output");
//
//    String[] args =
//      {"--inputpath=" + temp.toURI().toString(),
//       "--outputpath=" + output_path.toURI().toString(),
//       "--K=3",
//       "--randseed=12"};
//
//    try {
//      pair_merge.run(args);
//    } catch (Exception exception) {
//      fail("Exception occured:" + exception.getMessage());
//    }
//  }
}
