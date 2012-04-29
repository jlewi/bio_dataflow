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

// Extend PairMergeAvro so we can access mapper and reducer.
public class TestPairMergeAvro extends PairMergeAvro {

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

  // This class contains the data for testing the mapper.
  private static class MapperTestCase {
    public MapperTestCase() {
      input = new ArrayList<CompressibleNodeData>();
      expected_output =
          new HashMap<String, Pair<CharSequence, MergeNodeData>>();
      flipper = new CoinFlipperFixed();
    }
    public List<CompressibleNodeData> input;
    public HashMap<String, Pair<CharSequence, MergeNodeData>> expected_output;

    // The flipper to use in the test.
    public CoinFlipper flipper;
  }

  private MapperTestCase simpleMapperTest() {
    // Construct the simplest mapper test case.
    // We have two nodes. The first is assigned heads and the second tails.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTG", 3);

    MapperTestCase test_case = new MapperTestCase();
    for (GraphNode node: builder.getAllNodes().values()) {
      CompressibleNodeData data = new CompressibleNodeData();
      data.setNode(node.getData());

      data.setCompressibleStrands(CompressibleStrands.NONE);
      if (node.degree(DNAStrand.FORWARD) == 1 &&
          node.degree(DNAStrand.REVERSE) == 1) {
        data.setCompressibleStrands(CompressibleStrands.BOTH);
      } else if (node.degree(DNAStrand.FORWARD) == 1) {
        data.setCompressibleStrands(CompressibleStrands.FORWARD);
      } else if (node.degree(DNAStrand.REVERSE) == 1) {
        data.setCompressibleStrands(CompressibleStrands.REVERSE);
      }
      test_case.input.add(data);
    }

    // Make the first node the up node.
    GraphNode up_node = builder.getNode(builder.findNodeIdForSequence("ACT"));
    GraphNode down_node = builder.getNode(builder.findNodeIdForSequence("CTG"));

    CoinFlipperFixed flipper = new CoinFlipperFixed();
    test_case.flipper = flipper;
    flipper.tosses.put(up_node.getNodeId(), CoinFlipper.CoinFlip.Up);
    flipper.tosses.put(
        down_node.getNodeId(), CoinFlipper.CoinFlip.Down);

    MergeNodeData up_output = new MergeNodeData();
    up_output.setNode(up_node.clone().getData());
    up_output.setStrandToMerge(CompressibleStrands.FORWARD);

    // Up node is sent to the down node.
    test_case.expected_output.put(up_node.getNodeId(),
        new Pair<CharSequence, MergeNodeData>(down_node.getNodeId(), up_output));

    MergeNodeData down_output = new MergeNodeData();
    down_output.setNode(up_node.clone().getData());
    down_output.setStrandToMerge(CompressibleStrands.NONE);

    test_case.expected_output.put(down_node.getNodeId(),
        new Pair<CharSequence, MergeNodeData>(down_node.getNodeId(), down_output));
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
      test_case.input.add(data);

      MergeNodeData output = new MergeNodeData();
      output.setNode(node.clone().getData());
      output.setStrandToMerge(CompressibleStrands.NONE);
      // Up node is sent to the down node.
      test_case.expected_output.put(node.getNodeId(),
          new Pair<CharSequence, MergeNodeData>(node.getNodeId(), output));
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


    MapperTestCase test_case = new MapperTestCase();

    CoinFlipperFixed flipper = new CoinFlipperFixed();
    test_case.flipper = flipper;
    for (GraphNode node: builder.getAllNodes().values()) {
      CompressibleNodeData data = new CompressibleNodeData();
      data.setNode(node.getData());

      // Nodes are all compressible.
      data.setCompressibleStrands(
          isCompressible(builder.getAllNodes(), node.getNodeId()));
      test_case.input.add(data);

      MergeNodeData output = new MergeNodeData();
      output.setNode(node.clone().getData());

      // The output key for the reducer.
      String target_nodeid = "";
      if (node.getNodeId().equals("node_0")) {
        target_nodeid = "node_1";
        // Prefer merging along the forward strand.
        output.setStrandToMerge(CompressibleStrands.FORWARD);
      } else {
        target_nodeid = node.getNodeId();
        output.setStrandToMerge(CompressibleStrands.NONE);
      }
      // Up node is sent to the down node.
      test_case.expected_output.put(node.getNodeId(),
          new Pair<CharSequence, MergeNodeData>(target_nodeid, output));

      // All nodes assigned down.
      flipper.tosses.put(
          node.getNodeId(), CoinFlipper.CoinFlip.Down);
    }
    return test_case;
  }

  // Check the output of the mapper matches the expected result.
  private void assertMapperOutput(
      CompressibleNodeData input,
      Pair<CharSequence, MergeNodeData> expected_output,
      AvroCollectorMock<Pair<CharSequence, MergeNodeData>> collector_mock) {
    assertEquals(1, collector_mock.data.size());
    assertEquals(expected_output, collector_mock.data.get(0));
  }

  @Test
  public void testMapper() {
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();
    test_cases.add(mapperNoMergeTest());
    test_cases.add(simpleMapperTest());
    test_cases.add(mapperConvertDownToUpTest());

    PairMergeMapper mapper = new PairMergeMapper();

    JobConf job = new JobConf(PairMergeMapper.class);
    job.setLong("randseed", 11);

    mapper.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapperTestCase test_case: test_cases) {
      for (CompressibleNodeData input_data: test_case.input) {
        // We need a new collector for each invocation because the
        // collector stores the outputs of the mapper.
        AvroCollectorMock<Pair<CharSequence, MergeNodeData>> collector_mock =
          new AvroCollectorMock<Pair<CharSequence, MergeNodeData>>();

        mapper.setFlipper(test_case.flipper);
        try {
          mapper.map(
              input_data,collector_mock, reporter);
        }
        catch (IOException exception){
          fail("IOException occured in map: " + exception.getMessage());
        }

        assertMapperOutput(
            input_data,
            test_case.expected_output.get(input_data.getNode().getNodeId()),
            collector_mock);
      }
    }
  }

  private class ReducerTestCase {
    public int K;
    public String reducer_key;
    public List<MergeNodeData> input;
    public PairMergeOutput expected_output;
  }

  // Asserts that the output of the reducer is correct for this test case.
  private void assertReducerTestCase(
      ReducerTestCase test_case,
      AvroCollectorMock<PairMergeOutput> collector_mock) {

    assertEquals(1, collector_mock.data.size());
    PairMergeOutput output = collector_mock.data.get(0);

    assertEquals(test_case.expected_output, output);
  }

  private ReducerTestCase reducerNoMergeTest() {
    // Construct a simple reduce test case in which no nodes are merged.
    ReducerTestCase test_case = new ReducerTestCase();
    test_case.K = 3;

    test_case.input = new ArrayList<MergeNodeData>();
    GraphNode node = new GraphNode();
    node.setNodeId("somenode");
    Sequence sequence = new Sequence("ACGCT", DNAAlphabetFactory.create());
    node.setCanonicalSequence(sequence);
    test_case.reducer_key = node.getNodeId();

    MergeNodeData merge_data = new MergeNodeData();
    merge_data.setNode(node.clone().getData());
    merge_data.setStrandToMerge(CompressibleStrands.NONE);
    test_case.input.add(merge_data);

    test_case.expected_output = new PairMergeOutput();
    test_case.expected_output.setNode(node.clone().getData());
    test_case.expected_output.setUpdateMessages(
        new ArrayList<EdgeUpdateAfterMerge>());

    return test_case;
  }

  @Test
  public void testReducer() {
    ArrayList<ReducerTestCase> test_cases = new ArrayList<ReducerTestCase>();
    test_cases.add(reducerNoMergeTest());

    PairMergeReducer reducer = new PairMergeReducer();

    JobConf job = new JobConf(PairMergeReducer.class);

    // TODO: Reduce test cases can only use this value.
    job.setLong("K", 3);

    reducer.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (ReducerTestCase test_case: test_cases) {

      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<PairMergeOutput> collector_mock =
        new AvroCollectorMock<PairMergeOutput>();

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
}
