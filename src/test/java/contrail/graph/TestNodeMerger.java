package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.util.ListUtil;

public class TestNodeMerger extends NodeMerger {

  // Random number generator.
  private Random generator;
  @Before
  public void setUp() {
    // Create a random generator so we can make a test repeatable
    generator = new Random();
  }

  /**
   * Return a random sequence of characters of the specified length
   * using the given alphabet.
   *
   * @param length
   * @param alphabet
   * @return
   */
  public static String randomString(
      Random generator, int length, Alphabet alphabet) {
    // Generate a random sequence of the indicated length;
    char[] letters = new char[length];
    for (int pos = 0; pos < length; pos++) {
      // Randomly select the alphabet
      int rnd_int = generator.nextInt(alphabet.size());
      letters[pos] = alphabet.validChars()[rnd_int];
    }
    return String.valueOf(letters);
  }

  /**
   * Container for the data used to test merging sequences.
   */
  protected static class SequenceTestCase {
    public Sequence canonical_src;
    public Sequence canonical_dest;
    public StrandsForEdge strands;
    public int overlap;

    // Information about the merged sequence.
    // Merged sequence will be the actual sequence not the canonical sequence.
    public String merged_sequence;
    public DNAStrand merged_strand;

    public List<R5Tag> src_r5tags;
    public List<R5Tag> dest_r5tags;
    /**
     * We store the suffixes corresponding to the r5tags assigned
     * to each sequence. This way we can test whether the alignment is correct
     * after the merge.
     */
    HashMap<String, String> r5prefix;

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
    public static void createR5Tags(
        Random generator, String readid, int ntags, Sequence canonical,
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
    /**
     * Generate a random test case for the merging of two sequences.
     * @return
     */
    public static SequenceTestCase random(
        Random generator, int src_length, int dest_length, int overlap,
        int num_tags) {
      SequenceTestCase testcase = new SequenceTestCase();

      DNAStrand src_strand;
      {
        Sequence src;
        // Randomly generate a sequence for the source.
        src_strand = DNAStrandUtil.random(generator);
        String src_str = randomString(
            generator, src_length, DNAAlphabetFactory.create());
        src = new Sequence(src_str, DNAAlphabetFactory.create());

        testcase.canonical_src = DNAUtil.canonicalseq(src);
        testcase.merged_sequence =
            DNAUtil.sequenceToDir(
                testcase.canonical_src, src_strand).toString();
      }

      DNAStrand dest_strand;
      // Construct the destination sequence.
      {
        // Overlap by at most size(src) - 1 and at least 1.
        testcase.overlap = overlap;
        // Make a copy of canonical_src before we modify it.
        Sequence dest = new Sequence(testcase.canonical_src);
        dest = DNAUtil.sequenceToDir(dest, src_strand);
        dest = dest.subSequence(dest.size() - testcase.overlap, dest.size());

        String non_overlap = randomString(
            generator, dest_length - overlap, DNAAlphabetFactory.create());

        testcase.merged_sequence = testcase.merged_sequence + non_overlap;
        Sequence seq_nonoverlap = new Sequence(
            non_overlap, DNAAlphabetFactory.create());

        dest.add(seq_nonoverlap);

        dest_strand = DNAUtil.canonicaldir(dest);
        testcase.canonical_dest = DNAUtil.canonicalseq(dest);
      }

      Sequence merged_sequence = new Sequence(
          testcase.merged_sequence, DNAAlphabetFactory.create());
      testcase.merged_strand = DNAUtil.canonicaldir(merged_sequence);

      testcase.strands = StrandsUtil.form(src_strand, dest_strand);

      // Generate the R5Tags.
      testcase.src_r5tags = new ArrayList<R5Tag>();
      testcase.dest_r5tags = new ArrayList<R5Tag>();
      testcase.r5prefix = new HashMap<String, String>();

      // The validation of the tag alignment depends on the tags for the R5Tags
      // beginning with "src" and "dest" respectively.
      createR5Tags(
          generator, "src", num_tags, testcase.canonical_src,
          testcase.src_r5tags, testcase.r5prefix);

      createR5Tags(
          generator, "dest", num_tags, testcase.canonical_dest,
          testcase.dest_r5tags, testcase.r5prefix);
      return testcase;
    }

    /**
     * Generate a random test case for the merging of two sequences.
     * @return
     */
    public static SequenceTestCase random(Random generator) {
      int MAXLENGTH = 100;
      int MINLENGTH = 5;

      int src_length = generator.nextInt(MAXLENGTH - MINLENGTH) + MINLENGTH;

      int overlap = generator.nextInt(src_length - 2) + 1;
      int dest_length = generator.nextInt(MAXLENGTH - overlap) + overlap + 1;

      int num_tags = generator.nextInt(15) + 2;

      return SequenceTestCase.random(
          generator, src_length, dest_length, overlap, num_tags);
    }
  }

  /**
   * Check that NodeMerger.alignTags returns the correct result.
   * @param testcase
   * @param merge_info
   */
  protected void checkAlignTags(
      SequenceTestCase testcase, MergeInfo merge_info) {

    // We check the aligned tags.
    List<R5Tag> aligned = alignTags(
        merge_info, testcase.src_r5tags, testcase.dest_r5tags);

    int true_size = testcase.src_r5tags.size() + testcase.dest_r5tags.size();
    assertEquals(true_size, aligned.size());

    for (R5Tag tag: aligned) {
      String prefix = R5TagUtil.prefixForTag(
          merge_info.canonical_merged, tag).toString();

      // The prefix returned from the merge sequence could be longer
      // than the original prefix, so we only compare the length of
      // the original prefix.
      assertTrue(testcase.r5prefix.containsKey(tag.getTag().toString()));
      String true_prefix = testcase.r5prefix.get(tag.getTag().toString());

      assertTrue(prefix.startsWith(true_prefix));
    }
  }

  @Test
  public void testMergeSequences() {
    int ntrials = 20;
    for (int trial = 0; trial < ntrials; trial++) {
      SequenceTestCase testcase = SequenceTestCase.random(generator);
      MergeInfo merge_info;
      {
        // Make copies of the sequences so that merge won't modify them.
        Sequence src = new Sequence(testcase.canonical_src);
        Sequence dest = new Sequence(testcase.canonical_dest);
        merge_info =
            mergeSequences(src, dest, testcase.strands, testcase.overlap);
      }

      Sequence merged_sequence = DNAUtil.sequenceToDir(
          merge_info.canonical_merged, merge_info.merged_strand);
      assertEquals(testcase.merged_sequence, merged_sequence.toString());

      checkAlignTags(testcase, merge_info);
    }
  }

  protected static class NodesTestCase{
    public GraphNode src;
    public GraphNode dest;

    public int src_coverage_length;
    public int dest_coverage_length;

    public SequenceTestCase sequence_info;
  }

  protected static GraphNode createNode(
      String nodeid, Sequence sequence, Random generator) {
    long kMaxThreads = 1000;
    // Create a node with some edges.
    GraphNode node = new GraphNode();
    node.getData().setNodeId(nodeid);

    int num_edges = generator.nextInt(15);
    for (int edge_index = 0; edge_index < num_edges; edge_index++) {
      EdgeDirection direction = EdgeDirectionUtil.random(generator);
      List<CharSequence> tags = new ArrayList<CharSequence>();
      int num_tags = generator.nextInt(10);
      for (int tag_index = 0; tag_index < num_tags; tag_index++) {
        tags.add("tag_" + nodeid + "_" + edge_index + "_" + tag_index);
      }

      EdgeTerminal terminal = new EdgeTerminal(
          nodeid + ":" + edge_index, DNAStrandUtil.random(generator));

      DNAStrand strand = DNAStrandUtil.random(generator);
      // TODO(jlewi): Should we be adding bidirectional edges.
      if (direction == EdgeDirection.INCOMING) {
        node.addIncomingEdgeWithTags(strand, terminal, tags, kMaxThreads);
      } else {
        node.addOutgoingEdgeWithTags(strand, terminal, tags, kMaxThreads);
      }
    }
    node.setSequence(sequence);

    return node;
  }

  protected static NodesTestCase createMergeTestCase(
      SequenceTestCase sequence_case, Random generator) {
    NodesTestCase node_case = new NodesTestCase();
    node_case.sequence_info = sequence_case;

    node_case.src = createNode("src", sequence_case.canonical_src, generator);
    node_case.dest = createNode(
        "dest", sequence_case.canonical_dest, generator);

    // Set the coverage.
    node_case.src.setCoverage(generator.nextFloat() * 100);
    node_case.dest.setCoverage(generator.nextFloat() * 100);

    node_case.src_coverage_length = generator.nextInt(100) + 1;
    node_case.dest_coverage_length = generator.nextInt(100) + 1;

    // Add bidirectional edge to src between src and dest.
    {
      EdgeTerminal terminal = new EdgeTerminal(
          node_case.dest.getNodeId(), StrandsUtil.dest(sequence_case.strands));
      DNAStrand strand = StrandsUtil.src(sequence_case.strands);
      node_case.src.addOutgoingEdge(strand, terminal);
    }

    // Add bidirectional edge to dest between src and dest.
    {
      StrandsForEdge rcstrands = StrandsUtil.complement(sequence_case.strands);
      DNAStrand strand = DNAStrandUtil.flip(
          StrandsUtil.src(rcstrands));
      EdgeTerminal terminal = new EdgeTerminal(
          node_case.src.getNodeId(), StrandsUtil.dest(rcstrands));
      node_case.dest.addOutgoingEdge(strand, terminal);
    }

    return node_case;
  }

  /**
   * Check the coverage is set correctly.
   * @param testcase
   * @param merged
   */
  protected static void assertCoverage(
      NodesTestCase testcase, GraphNode merged) {
    double true_coverage =
        testcase.src_coverage_length * testcase.src.getCoverage() +
        testcase.dest_coverage_length * testcase.dest.getCoverage();
    double denominator =
        testcase.src_coverage_length + testcase.dest_coverage_length;
    true_coverage = true_coverage / denominator;

    // Compute the fractional difference.
    double err_fraction = Math.abs(merged.getCoverage() - true_coverage) /
                          true_coverage;

    double delta = 1E-4;
    assertEquals(0.0, err_fraction, delta);
  }

  /**
   * Check that the list of the tags associated with a set of
   * @param true_node: The original node.
   * @param true_strand: The strand to compare edge tags for.
   * @param merged_node: The merged node.
   * @param merged_strand: The strand of the merged node.
   * @param terminals: The terminals to compare.
   */
  protected  static void assertEdgeTagsAreEqual(
      GraphNode true_node, DNAStrand true_strand, GraphNode merged_node,
      DNAStrand merged_strand, List<EdgeTerminal> terminals) {

    // Check the threads associated with the edges.
    for (EdgeTerminal terminal: terminals) {
      List<CharSequence> expected_tags =
          true_node.getTagsForEdge(true_strand, terminal);
      List<CharSequence> tags =
          merged_node.getTagsForEdge(merged_strand, terminal);
      assertTrue(ListUtil.listsAreEqual(expected_tags, tags));
    }
  }

  protected static void assertEdgesForDirection(
      GraphNode expected_node, DNAStrand expected_strand, GraphNode merged,
      DNAStrand merged_strand, EdgeDirection direction) {

      // Check the terminal lists.
      List<EdgeTerminal> expected_incoming =
          expected_node.getEdgeTerminals(expected_strand, direction);
      List<EdgeTerminal> incoming =
          merged.getEdgeTerminals(merged_strand, direction);
      assertTrue(ListUtil.listsAreEqual(expected_incoming, incoming));

      // Check the threads associated with the edges match.
      DNAStrand expected_tag_strand = expected_strand;
      DNAStrand merged_tag_strand = merged_strand;
      List<EdgeTerminal> tag_terminals = new ArrayList<EdgeTerminal>();
      // For incoming edges we need to flip the strand that we get the tag for.
      if (direction == EdgeDirection.INCOMING) {
        expected_tag_strand = DNAStrandUtil.flip(expected_strand);
        merged_tag_strand = DNAStrandUtil.flip(merged_strand);

        // We also need to flip the strand for the terminals.
        for(EdgeTerminal terminal: expected_incoming) {
          tag_terminals.add(new EdgeTerminal(
              terminal.nodeId, DNAStrandUtil.flip(terminal.strand)));
        }
      } else {
        tag_terminals.addAll(expected_incoming);
      }

      assertEdgeTagsAreEqual(
          expected_node, expected_tag_strand, merged, merged_tag_strand,
          tag_terminals);
  }

  /**
   * Check the edges are set correctly.
   */
  protected static void assertEdges(NodesTestCase node_case, GraphNode merged) {
    {
      // Check the incoming edges for the edge corresponding
      // to the strands node_case.sequence_info.strands.
      GraphNode expected_node = node_case.src;
      DNAStrand expected_strand =
          StrandsUtil.src(node_case.sequence_info.strands);

      DNAStrand merged_strand = node_case.sequence_info.merged_strand;

      assertEdgesForDirection(
          expected_node, expected_strand, merged, merged_strand,
          EdgeDirection.INCOMING);
    }
    {
      // Check the outgoing edges.
      GraphNode expected_node = node_case.dest;
      DNAStrand expected_strand =
          StrandsUtil.dest(node_case.sequence_info.strands);

      DNAStrand merged_strand = node_case.sequence_info.merged_strand;

      assertEdgesForDirection(
          expected_node, expected_strand, merged, merged_strand,
          EdgeDirection.OUTGOING);
    }

    // Now do the same for the reverse complement.
    {
      StrandsForEdge rcstrands = StrandsUtil.complement(
          node_case.sequence_info.strands);
      GraphNode expected_node = node_case.dest;
      DNAStrand expected_strand =
          StrandsUtil.src(rcstrands);

      DNAStrand merged_strand =
          DNAStrandUtil.flip(node_case.sequence_info.merged_strand);

      assertEdgesForDirection(
          expected_node, expected_strand, merged, merged_strand,
          EdgeDirection.INCOMING);
    }
    {
      StrandsForEdge rcstrands = StrandsUtil.complement(
          node_case.sequence_info.strands);
      GraphNode expected_node = node_case.src;
      DNAStrand expected_strand =
          StrandsUtil.dest(rcstrands);

      DNAStrand merged_strand =
          DNAStrandUtil.flip(node_case.sequence_info.merged_strand);

      assertEdgesForDirection(
          expected_node, expected_strand, merged, merged_strand,
          EdgeDirection.OUTGOING);
    }
  }

  @Test
  public void testMergeNodes() {
    int ntrials = 20;
    for (int trial = 0; trial < ntrials; trial++) {
      SequenceTestCase sequence_testcase = SequenceTestCase.random(generator);

      NodesTestCase node_testcase = createMergeTestCase(
          sequence_testcase, generator);

      // Merge the nodes.
      MergeResult result = NodeMerger.mergeNodes(
          node_testcase.src, node_testcase.dest, sequence_testcase.strands,
          sequence_testcase.overlap, node_testcase.src_coverage_length,
          node_testcase.dest_coverage_length);

      // Check the sequence is set correctly.
      Sequence merged_sequence = DNAUtil.sequenceToDir(
          result.node.getSequence(),
          node_testcase.sequence_info.merged_strand);
      assertEquals(
          sequence_testcase.merged_sequence,
          merged_sequence.toString());

      // Check the coverage.
      assertCoverage(node_testcase, result.node);

      // Check the edges are correct.
      assertEdges(node_testcase, result.node);
    }
  }

  @Test
  public void testMergeNodeWithRC() {
    // Consider the special case  x->y->RC(y)->z
    // Can we sucessfully merge y and RC(y)
    // Note: RC(y)-> z  implies  RC(z) -> y
    // Note 2: There can't be other edges  a->RC(y)
    // because then we couldn't merge y and RC(y) we would have to remove
    // edge a->RC(y) before attempting the merge.
    SimpleGraphBuilder graph = new SimpleGraphBuilder();

    // Construct the chain TGG->GGC->GCC->CCC"
    final int K = 3;
    graph.addKMersForString("TGGCCC", K);

    MergeResult result;
    {
      // Get the nodes for the merge.
      EdgeTerminal src_terminal = graph.findEdgeTerminalForSequence("GGC");
      EdgeTerminal dest_terminal = graph.findEdgeTerminalForSequence("GCC");
      GraphNode src_node = graph.getNode(src_terminal.nodeId);
      GraphNode dest_node = graph.getNode(dest_terminal.nodeId);

      StrandsForEdge strands = StrandsUtil.form(
          src_terminal.strand, dest_terminal.strand);
      result = NodeMerger.mergeNodes(
          src_node, dest_node, strands, K - 1, 1, 1);
    }

    {
      // Check the result
      String merged_sequence = DNAUtil.sequenceToDir(
          result.node.getSequence(), result.strand).toString();
      assertEquals("GGCC", merged_sequence);
    }
    {
      // Check the outgoing edges.
      List<EdgeTerminal> expected_outgoing = new ArrayList<EdgeTerminal>();
      List<EdgeTerminal> outgoing =
          result.node.getEdgeTerminals(result.strand, EdgeDirection.OUTGOING);
      expected_outgoing.add(graph.findEdgeTerminalForSequence("CCC"));
      ListUtil.listsAreEqual(expected_outgoing, outgoing);
    }
    {
      // Check the incoming edges.
      List<EdgeTerminal> expected_incoming = new ArrayList<EdgeTerminal>();
      List<EdgeTerminal> incoming =
          result.node.getEdgeTerminals(result.strand, EdgeDirection.OUTGOING);
      expected_incoming.add(graph.findEdgeTerminalForSequence("TGG"));
      ListUtil.listsAreEqual(expected_incoming, incoming);
    }
  }
}
