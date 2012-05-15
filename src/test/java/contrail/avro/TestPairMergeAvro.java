package contrail.avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

// Extend PairMergeAvro so we can access the mapper and reducer.
public class TestPairMergeAvro extends PairMergeAvro {

//  // Return true if the strand of the specified node is compressible.
//  // This is used to setup some of the test cases.
//  private boolean isCompressibleStrand(
//      Map<String, GraphNode> nodes, String nodeid, DNAStrand strand) {
//    GraphNode node = nodes.get(nodeid);
//
//    if (node == null) {
//      fail("Could not find node:" + nodeid);
//    }
//
//    TailData tail = node.getTail(strand, EdgeDirection.OUTGOING);
//    if (tail == null) {
//      return false;
//    }
//
//    // Check if the other node has a tail in this direction.
//    GraphNode other_node = nodes.get(tail.terminal.nodeId);
//    if (other_node == null) {
//      fail("Could not find node:" + tail.terminal.nodeId);
//    }
//    TailData other_tail = other_node.getTail(
//        tail.terminal.strand, EdgeDirection.INCOMING);
//
//    if (other_tail == null) {
//      return false;
//    }
//
//    // Sanity check. The terminal for the other_node should be this node.
//    assertEquals(new EdgeTerminal(nodeid, strand), other_tail.terminal);
//
//    return true;
//  }
//
//  // Determine which strands for the given node are compressible.
//  private CompressibleStrands isCompressible(
//      Map<String, GraphNode> nodes, String nodeid) {
//    boolean f_compressible =
//        isCompressibleStrand(nodes, nodeid, DNAStrand.FORWARD);
//    boolean r_compressible =
//        isCompressibleStrand(nodes, nodeid, DNAStrand.FORWARD);
//
//    if (f_compressible && r_compressible) {
//      return CompressibleStrands.BOTH;
//    }
//
//    if (f_compressible) {
//      return CompressibleStrands.FORWARD;
//    }
//
//    if (r_compressible) {
//      return CompressibleStrands.REVERSE;
//    }
//
//    return CompressibleStrands.NONE;
//  }
//
//
  // This class serves as a container for the data for testing the mapper.
  private static class MapperTestCase {
    public MapperTestCase() {
      input = new NodeInfoForMerge();
    }
    // The input to the mapper.
    public NodeInfoForMerge input;

    // The expected key for the mapper output.
    public String key;
  }
//
  private MapperTestCase mapperNoMergeTest() {
    // Construct a map test case where a node gets sent to itself
    // because it isn't merged.
    MapperTestCase test_case = new MapperTestCase();
    GraphNode node = new GraphNode();
    node.setNodeId("some_node");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    compressible_node.setNode(node.getData());
    compressible_node.setCompressibleStrands(CompressibleStrands.NONE);

    test_case.input.setCompressibleNode(compressible_node);
    test_case.input.setStrandToMerge(CompressibleStrands.NONE);
    test_case.key = node.getNodeId();

    return test_case;
  }

  private MapperTestCase mapperMergeTest() {
    // Construct a map test case where a node gets merged along its reverse
    // strand. So the output is sent to the other terminal.
    MapperTestCase test_case = new MapperTestCase();
    GraphNode node = new GraphNode();
    node.setNodeId("some_node");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));

    EdgeTerminal terminal = new EdgeTerminal("terminal", DNAStrand.REVERSE);
    node.addOutgoingEdge(DNAStrand.REVERSE, terminal);

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    compressible_node.setNode(node.getData());
    compressible_node.setCompressibleStrands(CompressibleStrands.BOTH);

    test_case.input.setCompressibleNode(compressible_node);
    test_case.input.setStrandToMerge(CompressibleStrands.REVERSE);
    test_case.key = terminal.nodeId;

    return test_case;
  }
//  private MapperTestCase mapperConvertDownToUpTest() {
//    // Construct a chain of 3 nodes all assigned down.
//    // Check that we properly convert the node in the middle to up.
//    SimpleGraphBuilder builder = new SimpleGraphBuilder();
//
//    // Add the three nodes. We need to assign the node in the middle
//    // a node id less than the two terminals.
//    builder.addNode("node_1", "ACT");
//    builder.addNode("node_0", "CTG");
//    builder.addNode("node_2", "TGA");
//
//    builder.addEdge("ACT", "CTG", 2);
//    builder.addEdge("CTG", "TGA", 2);
//
//    MapperTestCase test_case = new MapperTestCase();
//
//    CoinFlipperFixed flipper = new CoinFlipperFixed();
//    test_case.flipper = flipper;
//    for (GraphNode node: builder.getAllNodes().values()) {
//      CompressibleNodeData data = new CompressibleNodeData();
//      data.setNode(node.getData());
//
//      // Nodes are all compressible.
//      data.setCompressibleStrands(
//          isCompressible(builder.getAllNodes(), node.getNodeId()));
//      test_case.input.add(data);
//
//      CompressibleNodeData output = new CompressibleNodeData();
//      output.setNode(node.clone().getData());
//
//      // The output key for the reducer.
//      String target_nodeid = "";
//      if (node.getNodeId().equals("node_0")) {
//        target_nodeid = "node_1";
//        output.setCompressibleStrands(CompressibleStrands.BOTH);
//      } else if (node.getNodeId().equals("node_1")) {
//        target_nodeid = node.getNodeId();
//        output.setCompressibleStrands(CompressibleStrands.FORWARD);
//      } else if (node.getNodeId().equals("node_2")) {
//        target_nodeid = node.getNodeId();
//        output.setCompressibleStrands(CompressibleStrands.FORWARD);
//      }
//      // Up node is sent to the down node.
//      test_case.expected_output.put(node.getNodeId(),
//          new Pair<CharSequence, CompressibleNodeData>(target_nodeid, output));
//
//      // All nodes assigned down.
//      flipper.tosses.put(
//          node.getNodeId(), CoinFlipper.CoinFlip.Down);
//    }
//    return test_case;
//  }
//
  // Check the output of the mapper matches the expected result.
  private void assertMapperOutput(
      MapperTestCase test_case,
      AvroCollectorMock<Pair<CharSequence, NodeInfoForMerge>> collector_mock) {
    assertEquals(1, collector_mock.data.size());
    Pair<CharSequence, NodeInfoForMerge> out_pair = collector_mock.data.get(0);
    assertEquals(test_case.key, out_pair.key().toString());
    assertEquals(test_case.input, out_pair.value());
  }

  @Test
  public void testMapper() {
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();
    test_cases.add(mapperNoMergeTest());
    test_cases.add(mapperMergeTest());
    PairMergeMapper mapper = new PairMergeMapper();

    JobConf job = new JobConf(PairMergeMapper.class);
    job.setLong("K", 3);

    mapper.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapperTestCase test_case: test_cases) {
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<CharSequence, NodeInfoForMerge>>
        collector_mock =
        new AvroCollectorMock<Pair<CharSequence, NodeInfoForMerge>>();

      try {
        mapper.map(
            test_case.input, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertMapperOutput(test_case, collector_mock);

    }
  }
//
//  // A container class used for organizing the data for the reducer tests.
//  private class ReducerTestCase {
//    public int K;
//    public String reducer_key;
//    // The input to the reducer.
//    public List<CompressibleNodeData> input;
//    // The expected output from the reducer.
//    public PairMergeOutput expected_output;
//  }
//
//  // Asserts that the output of the reducer is correct for this test case.
//  private void assertReducerTestCase(
//      ReducerTestCase test_case,
//      AvroCollectorMock<PairMergeOutput> collector_mock) {
//
//    assertEquals(1, collector_mock.data.size());
//    PairMergeOutput output = collector_mock.data.get(0);
//
//    // Check the nodes are equal.
//    assertEquals(
//        test_case.expected_output.getCompressibleNode(),
//        output.getCompressibleNode());
//
//    // Check the lists are equal without regard to order.
//    assertTrue(ListUtil.listsAreEqual(
//        test_case.expected_output.getUpdateMessages(),
//        output.getUpdateMessages()));
//  }
//
//  private ReducerTestCase reducerNoMergeTest() {
//    // Construct a simple reduce test case in which no nodes are merged.
//    ReducerTestCase test_case = new ReducerTestCase();
//    test_case.K = 3;
//
//    test_case.input = new ArrayList<CompressibleNodeData>();
//    GraphNode node = new GraphNode();
//    node.setNodeId("somenode");
//    Sequence sequence = new Sequence("ACGCT", DNAAlphabetFactory.create());
//    node.setCanonicalSequence(sequence);
//    test_case.reducer_key = node.getNodeId();
//
//    CompressibleNodeData merge_data = new CompressibleNodeData();
//    merge_data.setNode(node.clone().getData());
//    merge_data.setCompressibleStrands(CompressibleStrands.NONE);
//    test_case.input.add(merge_data);
//
//    test_case.expected_output = new PairMergeOutput();
//    test_case.expected_output.setCompressibleNode(
//        copyCompressibleNode(merge_data));
//    test_case.expected_output.setUpdateMessages(
//        new ArrayList<EdgeUpdateAfterMerge>());
//
//    return test_case;
//  }
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
//  @Test
//  public void testReducer() {
//    ArrayList<ReducerTestCase> test_cases = new ArrayList<ReducerTestCase>();
//    test_cases.add(reducerNoMergeTest());
//    test_cases.add(reducerSimpleMergeTest());
//    test_cases.add(reducerTwoMergeTest());
//    PairMergeReducer reducer = new PairMergeReducer();
//
//    JobConf job = new JobConf(PairMergeReducer.class);
//
//    // TODO: Reduce test cases can only use this value.
//    job.setLong("K", 3);
//
//    reducer.configure(job);
//
//    ReporterMock reporter_mock = new ReporterMock();
//    Reporter reporter = reporter_mock;
//
//    for (ReducerTestCase test_case: test_cases) {
//
//      // We need a new collector for each invocation because the
//      // collector stores the outputs of the mapper.
//      AvroCollectorMock<PairMergeOutput> collector_mock =
//        new AvroCollectorMock<PairMergeOutput>();
//
//      try {
//        reducer.reduce(
//            test_case.reducer_key, test_case.input, collector_mock, reporter);
//      }
//      catch (IOException exception){
//        fail("IOException occured in map: " + exception.getMessage());
//      }
//
//      assertReducerTestCase(test_case, collector_mock);
//    }
//  }
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
