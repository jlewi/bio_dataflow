package contrail.graph;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

public class TestNodeReverser {
  private Random generator;
  @Before
  public void setUp() {
    generator = new Random();
  }

  /**
   * Class to store the data for the test case.
   */
  private static class TestCase {
    public GraphNode node;
    // Keep track of the prefixes corresponding to the R5Tags.
    public HashMap<String, String> prefixes;
    public TestCase() {
      prefixes = new HashMap<String, String>();
    }
  }
  /**
   * @return A random node to use in the tests.
   */
  private GraphNode createNode() {
    GraphNode node = new GraphNode();
    node.setCoverage(generator.nextFloat() * 100);
    node.setNodeId("node");

    String random_sequence =
        AlphabetUtil.randomString(generator, 10, DNAAlphabetFactory.create());
    Sequence sequence = new Sequence(
        random_sequence, DNAAlphabetFactory.create());
    node.setSequence(sequence);

    int num_dest_nodes = generator.nextInt(30) + 5;
    for (int index = 0; index < num_dest_nodes; index++) {
      // Generate an edge.
      StrandsForEdge strands = StrandsForEdge.values()[
          generator.nextInt(StrandsForEdge.values().length)];

      EdgeTerminal terminal = new EdgeTerminal(
          "dest_" + index, StrandsUtil.dest(strands));
      node.addOutgoingEdge(StrandsUtil.src(strands), terminal);
    }
    return node;
  }

  /**
   * Create some R5Tags for this sequence.
   * @param readid: Id for the read. Used so we can match up the aligned
   *   value with this read.
   * @param ntags: The number of tags.
   * @param canonical: The canonical sequence.
   * @param r5tags: List to add the tags to.
   * @param prefixes: The prefixes of the reads that are aligned with this
   *   sequence.
   */
  private  void createR5Tags(
      String readid, int ntags, Sequence canonical,
      List<R5Tag> r5tags, HashMap<String, String> prefixes) {
    for (int i = 0; i < ntags; i++) {
      // Randomly select the offset and strand.
      R5Tag tag = new R5Tag();
      r5tags.add(tag);
      tag.setTag(readid + ":" + i);
      tag.setOffset(generator.nextInt(canonical.size()));
      tag.setStrand(DNAStrandUtil.random(generator));

      String prefix = R5TagUtil.prefixForTag(canonical,tag).toString();
      prefixes.put(tag.getTag().toString(), prefix);
    }
  }

  private TestCase randomCase() {
    TestCase test_case = new TestCase();
    test_case.node = createNode();

    createR5Tags(
        "read_1", 3, test_case.node.getSequence(),
        test_case.node.getData().getR5Tags(), test_case.prefixes);
    createR5Tags(
        "read_2", 3, test_case.node.getSequence(),
        test_case.node.getData().getR5Tags(), test_case.prefixes);
    return test_case;
  }

  /**
   * Check that the reversed node is correct.
   * @param test_case
   * @param reversed
   */
  private void assertReversedNode(TestCase test_case, GraphNode reversed) {
    // Check the sequence.
    assertEquals(
        test_case.node.getSequence(),
        DNAUtil.reverseComplement(reversed.getSequence()));

    // Check the edges.
    GraphNodeData input_data = test_case.node.getData();
    GraphNodeData output_data = reversed.getData();
    assertEquals(
        input_data.getNeighbors().size(), output_data.getNeighbors().size());
    for (int i = 0; i < input_data.getNeighbors().size(); ++i) {
      List<EdgeData> input_edges = input_data.getNeighbors().get(i).getEdges();
      List<EdgeData> output_edges =
          output_data.getNeighbors().get(i).getEdges();
      assertEquals(input_edges.size(), output_edges.size());
      int num_edges = input_edges.size();
      for (int eindex = 0; eindex < num_edges; ++eindex) {
        EdgeData input_edge =  input_edges.get(eindex);
        EdgeData output_edge = output_edges.get(eindex);

        // Check the strands are reversed.
        assertEquals(
            StrandsUtil.src(input_edge.getStrands()),
            DNAStrandUtil.flip(StrandsUtil.src(output_edge.getStrands())));
        assertEquals(
            StrandsUtil.dest(input_edge.getStrands()),
            StrandsUtil.dest(output_edge.getStrands()));
      }
    }

    // Check the R5 tags.
    assertEquals(input_data.getR5Tags().size(), output_data.getR5Tags().size());
    for (int i = 0; i < input_data.getR5Tags().size(); ++i) {
      R5Tag in_tag = input_data.getR5Tags().get(i);
      R5Tag out_tag = output_data.getR5Tags().get(i);

      // Check the prefixes match.
      Sequence in_prefix = R5TagUtil.prefixForTag(
          test_case.node.getSequence(), in_tag);
      Sequence out_prefix = R5TagUtil.prefixForTag(
          reversed.getSequence(), out_tag);
      assertEquals(in_prefix, out_prefix);
      assertEquals(
          test_case.prefixes.get(in_tag.getTag()), out_prefix.toString());

      assertEquals(in_tag.getStrand(), DNAStrandUtil.flip(out_tag.getStrand()));
      assertEquals(
          in_tag.getOffset().intValue(),
          test_case.node.getSequence().size() - out_tag.getOffset() - 1);
    }

    // Finally, reverse the output node and check it equals
    // the original input. This should ensure all fields are set properly.
    NodeReverser reverser = new NodeReverser();
    GraphNode match_input =  reverser.reverse(reversed);
    assertEquals(input_data, match_input.getData());
  }

  @Test
  public void testReverseNode() {
    int num_trials = 10;
    NodeReverser reverser = new NodeReverser();
    for (int trial = 0; trial < num_trials; trial++) {
      TestCase test_case = randomCase();
      GraphNode reversed = reverser.reverse(test_case.node);
      assertReversedNode(test_case, reversed);
    }
  }
}
