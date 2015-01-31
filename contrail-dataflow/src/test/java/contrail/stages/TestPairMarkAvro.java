// Author: Jeremy Lewi
package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.graph.TailData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.stages.CoinFlipper.CoinFlip;

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

    @Override
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

  private List<MapperTestCase> mapperCycleWithIncomingEdge() {
    // This test covers a subgraph that we saw in the staph dataset. The
    // graph is  B->X->RC(Y)->X .
    // In this case the cycle is automatically broken at X because of
    // the incoming edge from B. This means we will try to merge X & Y
    // So the resulting graph should be B->{XY}.
    // Since X & Y are the nodes that will be merged they should not
    // send updates to each other to move the edges to each other in the
    // reducer because this will happen in PairMerge.
    GraphNode branchNode = new GraphNode();
    branchNode.setNodeId("branch");
    branchNode.setSequence(new Sequence("ACCT", DNAAlphabetFactory.create()));

    GraphNode cycleStart = new GraphNode();
    cycleStart.setNodeId("cyclestart");
    cycleStart.setSequence(new Sequence("CTC", DNAAlphabetFactory.create()));

    GraphNode cycleEnd = new GraphNode();
    cycleEnd.setNodeId("cyclend");
    cycleEnd.setSequence(
        DNAUtil.reverseComplement(
            new Sequence("TCCT", DNAAlphabetFactory.create())));

    GraphUtil.addBidirectionalEdge(
        branchNode, DNAStrand.FORWARD, cycleStart, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        cycleStart, DNAStrand.FORWARD, cycleEnd, DNAStrand.REVERSE);

    GraphUtil.addBidirectionalEdge(
        cycleEnd, DNAStrand.REVERSE, cycleStart, DNAStrand.FORWARD);

    ArrayList<MapperTestCase> testCases = new ArrayList<MapperTestCase>();
    {
      MapperTestCase testCase = new MapperTestCase();
      // In the first case assume X is up so it gets sent to Y.
      CoinFlipperFixed flipper = new CoinFlipperFixed();
      testCase.flipper = flipper;
      flipper.tosses.put(cycleStart.getNodeId(), CoinFlipper.CoinFlip.UP);
      flipper.tosses.put(cycleEnd.getNodeId(), CoinFlipper.CoinFlip.DOWN);

      MapperInputOutput inputOutput = new MapperInputOutput();

      inputOutput.input_node = new CompressibleNodeData();
      inputOutput.input_node.setNode(cycleStart.clone().getData());
      inputOutput.input_node.setCompressibleStrands(
          CompressibleStrands.FORWARD);

      // The first output is the node itself.
      inputOutput.output_node = new NodeInfoForMerge();

      CompressibleNodeData outNode = new CompressibleNodeData();
      outNode.setNode(cycleStart.clone().getData());
      outNode.setCompressibleStrands(CompressibleStrands.FORWARD);
      inputOutput.output_node.setCompressibleNode(outNode);
      inputOutput.output_node.setStrandToMerge(CompressibleStrands.FORWARD);

      // Second output is a message to branch telling it about the impending
      // move.
      EdgeUpdateForMerge edgeUpdate = new EdgeUpdateForMerge();
      edgeUpdate.setNewStrand(DNAStrand.REVERSE);
      edgeUpdate.setNewId(cycleEnd.getNodeId());
      edgeUpdate.setOldId(cycleStart.getNodeId());
      edgeUpdate.setOldStrand(DNAStrand.FORWARD);
      inputOutput.edge_updates.put(branchNode.getNodeId(), edgeUpdate);
      testCase.inputs_outputs.put(cycleStart.getNodeId(), inputOutput);

      testCases.add(testCase);
    }

    {
      MapperTestCase testCase = new MapperTestCase();
      // In the second case assume Y is down so it gets sent to X.
      CoinFlipperFixed flipper = new CoinFlipperFixed();
      testCase.flipper = flipper;
      flipper.tosses.put(cycleStart.getNodeId(), CoinFlipper.CoinFlip.DOWN);
      flipper.tosses.put(cycleEnd.getNodeId(), CoinFlipper.CoinFlip.UP);

      MapperInputOutput inputOutput = new MapperInputOutput();

      inputOutput.input_node = new CompressibleNodeData();
      inputOutput.input_node.setNode(cycleEnd.clone().getData());
      inputOutput.input_node.setCompressibleStrands(
          CompressibleStrands.BOTH);

      // The only output is the node itself because all of its edges are
      // to the node it will be merged with.
      inputOutput.output_node = new NodeInfoForMerge();

      CompressibleNodeData outNode = new CompressibleNodeData();
      outNode.setNode(cycleEnd.clone().getData());
      outNode.setCompressibleStrands(CompressibleStrands.BOTH);
      inputOutput.output_node.setCompressibleNode(outNode);
      inputOutput.output_node.setStrandToMerge(CompressibleStrands.FORWARD);

      testCase.inputs_outputs.put(cycleEnd.getNodeId(), inputOutput);
      testCases.add(testCase);
    }
    return testCases;
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
    test_cases.addAll(mapperCycleWithIncomingEdge());

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
    // The graph is:
    //  somenode:F -> node2:R
    //  node2:R -> node3:F
    // node2 is the node being merged away so it sends an update message
    // to somenode.
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
    input_data.setCompressibleStrands(CompressibleStrands.FORWARD);
    NodeInfoForMerge input_node = new NodeInfoForMerge();
    input_node.setCompressibleNode(input_data);

    // This node isn't being merged.
    input_node.setStrandToMerge(CompressibleStrands.NONE);

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
        CompressibleStrands.FORWARD);
    test_case.expected_output.setStrandToMerge(CompressibleStrands.NONE);

    return test_case;
  }

  private void runReducerTests(ArrayList<ReducerTestCase> testCases) {
    PairMarkReducer reducer = new PairMarkReducer();

    JobConf job = new JobConf(PairMarkReducer.class);

    reducer.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (ReducerTestCase test_case: testCases) {
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
  public void testReducer() {
    ArrayList<ReducerTestCase> test_cases = new ArrayList<ReducerTestCase>();
    test_cases.add(reducerNoUpdatesTest());
    test_cases.add(reducerUpdateTest());
    runReducerTests(test_cases);
  }

  @Test
  public void testReducerCycle() {
    // This test covers an actual problem that came up with the rhodobacter
    // dataset.
    // Suppose we have a cycle A->B->C->A. Suppose A and B are assigned
    // UP so they get sent to C to be merged. Then A and B will send messages
    // to each other to move the edges to each other so they point to
    // what will be the new merged node. However, we don't want to move these
    // edges because it will be much easier to detect and handle the cycle
    // in PairMerge avro where we will have access to all the ndoes.
    // So in PairMark.Reduce when processing A we shouldn't update the
    // edge to B.
    ReducerTestCase testCase = new ReducerTestCase();

    testCase.input = new ArrayList<PairMarkOutput>();
    GraphNode node = new GraphNode();
    node.setNodeId("nodeA");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));

    // Add an incoming edge to A from node C.
    EdgeTerminal cTerminal = new EdgeTerminal("nodeC", DNAStrand.FORWARD);
    node.addIncomingEdge(DNAStrand.FORWARD, cTerminal);

    // Add an outgoing edge to B.
    EdgeTerminal bTerminal = new EdgeTerminal("nodeB", DNAStrand.FORWARD);
    node.addOutgoingEdge(DNAStrand.FORWARD, bTerminal);

    CompressibleNodeData compressibleNode = new CompressibleNodeData();
    compressibleNode.setNode(node.getData());
    compressibleNode.setCompressibleStrands(CompressibleStrands.BOTH);

    NodeInfoForMerge mergeInfo = new NodeInfoForMerge();
    mergeInfo.setCompressibleNode(compressibleNode);

    // Node a would be marked for compression along its reverse strand
    // because the compressible strand always refers to an outgoing edge.
    mergeInfo.setStrandToMerge(CompressibleStrands.REVERSE);

    PairMarkOutput aOutput = new PairMarkOutput();
    aOutput.setPayload(mergeInfo);
    testCase.input.add(aOutput);

    // Node B sends a message to A to move the edge from A to C since
    // B will be merged with C.
    EdgeUpdateForMerge edgeUpdate = new EdgeUpdateForMerge();
    edgeUpdate.setNewId(cTerminal.nodeId);
    edgeUpdate.setNewStrand(cTerminal.strand);
    edgeUpdate.setOldId(bTerminal.nodeId);
    edgeUpdate.setOldStrand(bTerminal.strand);

    // The expected node is just the original node because none of its edges
    // should have been moved.
    testCase.expected_output =
        CompressUtil.copyNodeInfoForMerge(mergeInfo);

    ArrayList<ReducerTestCase> cases = new ArrayList<ReducerTestCase>();
    cases.add(testCase);
    runReducerTests(cases);
  }

  /**
   * Run the complete MR job without doing anything funky.
   *
   * @param inputNodes
   */
  private void runJob(Collection<CompressibleNodeData> inputNodes) {
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
      for (CompressibleNodeData node : inputNodes) {
        writer.append(node);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }

    // Run it.
    PairMarkAvro stage = new PairMarkAvro();
    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
       "--randseed=12"};

    try {
      stage.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }

  @Test
  public void testRun() {
    MapperTestCase testCase = this.mapperConvertDownToUpTest();
    ArrayList<CompressibleNodeData> inputNodes =
        new ArrayList<CompressibleNodeData>();

    for (MapperInputOutput inOut : testCase.inputs_outputs.values()) {
      inputNodes.add(inOut.input_node);
    }
    // This function tests that we can run the MR job without errors.
    // It doesn't test for correctness.
    runJob(inputNodes);
  }

  private static class RunResults {
    public RunResults() {
      reduceOutputs = new HashMap<String, NodeInfoForMerge>();
    }

    public HashMap<String, ArrayList<PairMarkOutput>> mapOutputs;
    public HashMap<String, NodeInfoForMerge> reduceOutputs;
  }

  /**
   * Run the mapper and reducer but use the flipper passed in.
   */
  private RunResults runWithFlipper(
      Collection<CompressibleNodeData> inputs, CoinFlipper flipper) {

    PairMarkMapper mapper = new PairMarkMapper();

    JobConf job = new JobConf(PairMarkMapper.class);
    PairMarkAvro stage = new PairMarkAvro();
    Map<String, ParameterDefinition> parameters =
        stage.getParameterDefinitions();
    parameters.get("randseed").addToJobConf(job, new Long(1));

    mapper.configure(job);
    mapper.setFlipper(flipper);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    AvroCollectorMock<Pair<CharSequence, PairMarkOutput>> collector =
      new AvroCollectorMock<Pair<CharSequence, PairMarkOutput>>();

    for (CompressibleNodeData inputNode: inputs) {
      try {
        mapper.map( inputNode, collector, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
    }

    // Group the mapper outputs by key.
    HashMap<String, ArrayList<PairMarkOutput>> reducerPairs =
        new HashMap<String, ArrayList<PairMarkOutput>>();

    for (Pair<CharSequence, PairMarkOutput> pair : collector.data) {
      String key = pair.key().toString();
      if (!reducerPairs.containsKey(key)) {
        reducerPairs.put(key,  new ArrayList<PairMarkOutput>());
      }
      reducerPairs.get(key).add(pair.value());
    }

    PairMarkReducer reducer = new PairMarkReducer();
    reducer.configure(new JobConf(PairMarkReducer.class));

    AvroCollectorMock<NodeInfoForMerge> reducerCollector =
        new AvroCollectorMock<NodeInfoForMerge>();

    ReporterMock reducerReporter = new ReporterMock();

    for (String key : reducerPairs.keySet()) {
      try {
        reducer.reduce(
            key, reducerPairs.get(key), reducerCollector, reducerReporter);
      } catch (IOException e) {
        fail(e.getMessage());
      }
    }

    RunResults results = new RunResults();
    results.mapOutputs = reducerPairs;

    for (NodeInfoForMerge output : reducerCollector.data) {
      String key =
          output.getCompressibleNode().getNode().getNodeId().toString();
      // Each node should appear once in the output
      assertTrue(!results.reduceOutputs.containsKey(key));
      results.reduceOutputs.put(key, output);
    }

    return results;
  }

  /**
   * Helper function for comparing two NodeInfoForMerge.
   *
   * @param expected
   * @param actual
   */
  private void assertNodeInfoForMergeEqual(
      NodeInfoForMerge expected, NodeInfoForMerge actual) {
    assertEquals(expected.getStrandToMerge(), actual.getStrandToMerge());
    assertEquals(
        expected.getCompressibleNode().getCompressibleStrands(),
        actual.getCompressibleNode().getCompressibleStrands());
    GraphNode expectedNode = new GraphNode(
        expected.getCompressibleNode().getNode());
    GraphNode actualNode = new GraphNode(
        actual.getCompressibleNode().getNode());
    GraphNode.NodeDiff diff = expectedNode.equalsWithInfo(actualNode);
    assertEquals(GraphNode.NodeDiff.NONE, diff);
  }

  @Test
  public void testPalindrome() {
    // We test a particular case involving palindromes.
    // Palindromes should be handled like any other case and this test confirms
    // that.
    // Suppose we have A->B->C->D
    // B is a down node. A, C are up nodes (D isn't compressible).
    // Suppose B is a down node and we are merging it with C.
    // B preserves its strand and id so C sends a message to D telling D to
    // move the edge C->D to the new new node BC.
    //
    // TODO(jeremy@lewi.us) should we run the test twice once where C
    // uses the forward strand and the other where it uses the reverse strand?
    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "AAC");
    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "ACA");
    GraphNode nodeC = GraphTestUtil.createNode("nodeC", "CATG");
    GraphNode nodeD = GraphTestUtil.createNode("nodeD", "TGG");

    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeB, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeB, DNAStrand.FORWARD, nodeC, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        nodeC, DNAStrand.FORWARD, nodeD, DNAStrand.FORWARD);

    CompressibleNodeData nodeACompressible = new CompressibleNodeData();
    nodeACompressible.setNode(nodeA.getData());
    nodeACompressible.setCompressibleStrands(CompressibleStrands.FORWARD);

    CompressibleNodeData nodeBCompressible = new CompressibleNodeData();
    nodeBCompressible.setNode(nodeB.getData());
    nodeBCompressible.setCompressibleStrands(CompressibleStrands.BOTH);

    // CompressibleAvro always marks the forward strand as the compressible
    // strand for a palindrome. This field should be ignored if its a
    // palindrome.
    CompressibleNodeData nodeCCompressible = new CompressibleNodeData();
    nodeCCompressible.setNode(nodeC.getData());
    nodeCCompressible.setCompressibleStrands(CompressibleStrands.REVERSE);

    // We mark node D as not being compressible. We didn't bother adding
    // edges to make it truly not compressible.
    CompressibleNodeData nodeDCompressible = new CompressibleNodeData();
    nodeDCompressible.setNode(nodeD.getData());
    nodeDCompressible.setCompressibleStrands(CompressibleStrands.NONE);

    CoinFlipperFixed flipper = new CoinFlipperFixed();
    flipper.tosses.put(nodeA.getNodeId(), CoinFlip.UP);
    flipper.tosses.put(nodeB.getNodeId(), CoinFlip.DOWN);
    flipper.tosses.put(nodeC.getNodeId(), CoinFlip.UP);
    flipper.tosses.put(nodeD.getNodeId(), CoinFlip.DOWN);

    RunResults results = runWithFlipper(
        Arrays.asList(
            nodeACompressible, nodeBCompressible, nodeCCompressible,
            nodeDCompressible),
        flipper);

    // Node A should be marked to be compressed with Node B.
    NodeInfoForMerge infoA = new NodeInfoForMerge();
    infoA.setCompressibleNode(nodeACompressible);
    infoA.setStrandToMerge(CompressibleStrands.FORWARD);

    // Since B is the down node nothing happens.
    NodeInfoForMerge infoB = new NodeInfoForMerge();
    infoB.setCompressibleNode(nodeBCompressible);
    infoB.setStrandToMerge(CompressibleStrands.NONE);

    // C gets sent to B and sends a message to D.
    NodeInfoForMerge infoC = new NodeInfoForMerge();
    infoC.setCompressibleNode(nodeCCompressible);
    infoC.setStrandToMerge(CompressibleStrands.REVERSE);

    // NodeD should have its edge from C moved to point to the new merged
    // node. The merged sequence for A->B->C will be AACATG.
    // So clearly the edge to D("TG") is from the forward strand of the
    // merged node.
    GraphNode expectedD = GraphTestUtil.createNode(
        "nodeD", nodeD.getSequence().toString());
    expectedD.addIncomingEdge(
        DNAStrand.FORWARD,
        new EdgeTerminal(nodeB.getNodeId(), DNAStrand.FORWARD));

    CompressibleNodeData expectedDCompressible = new CompressibleNodeData();
    expectedDCompressible.setNode(expectedD.getData());
    expectedDCompressible.setCompressibleStrands(CompressibleStrands.NONE);

    NodeInfoForMerge infoD = new NodeInfoForMerge();
    infoD.setCompressibleNode(expectedDCompressible);
    infoD.setStrandToMerge(CompressibleStrands.NONE);

    assertNodeInfoForMergeEqual(infoA, results.reduceOutputs.get(nodeA.getNodeId()));
    assertNodeInfoForMergeEqual(infoB, results.reduceOutputs.get(nodeB.getNodeId()));
    assertNodeInfoForMergeEqual(infoC, results.reduceOutputs.get(nodeC.getNodeId()));
    assertNodeInfoForMergeEqual(infoD, results.reduceOutputs.get(nodeD.getNodeId()));
  }
}
