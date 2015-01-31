package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.graph.GraphNode.NodeDiff;
import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.util.ListUtil;

public class TestNodeMerger extends NodeMerger {
  @Test
  public void testMergeSequences() {
    // Create a random sequences.
    int overlap = 13;
    int numNodes = 23;

    // HashMap to keep track of R5Tags
    HashMap<String, String> r5Prefixes = new HashMap<String, String>();
    int ntags = 2;

    // Create a chain. We do this by generating numNodes random sequences.
    // We then append the last overlap characters from the previous node
    // to the start of the next node.
    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
    ArrayList<EdgeTerminal> terminals = new ArrayList<EdgeTerminal>();

    HashMap<String, GraphNode> nodesMap = new HashMap<String, GraphNode>();
    String trueSequence = "";
    for (int nindex = 0; nindex < numNodes; ++nindex) {
      GraphNode newNode = new GraphNode();
      newNode.setNodeId("node" + nindex);
      String dna = "";

      if (nindex > 0) {
        Sequence previousSequence = nodes.get(nindex - 1).getSequence();
        EdgeTerminal previousTerminal = terminals.get(nindex - 1);
        previousSequence = DNAUtil.sequenceToDir(
            previousSequence, previousTerminal.strand);
        dna = previousSequence.toString().substring(
            previousSequence.size() - overlap, previousSequence.size());
      }

      String newDna = AlphabetUtil.randomString(
          generator, overlap + 1, DNAAlphabetFactory.create());

      trueSequence += newDna;
      dna += newDna;

      Sequence sequence = new Sequence(dna, DNAAlphabetFactory.create());
      GraphNode node =  new GraphNode();
      node.setNodeId("node" + nindex);
      node.setSequence(DNAUtil.canonicalseq(sequence));

      // Create some R5Tags.
      SequenceTestCase.createR5Tags(
          generator, "read_" + nindex, ntags, node.getSequence(),
          node.getData().getR5Tags(), r5Prefixes);

      nodes.add(node);
      nodesMap.put(node.getNodeId(), node);
      EdgeTerminal terminal = new EdgeTerminal(
          node.getNodeId(), DNAUtil.canonicaldir(sequence));
      terminals.add(terminal);
    }

    NodeMerger merger = new NodeMerger();

    Sequence mergedSequence = merger.mergeSequences(terminals, nodesMap, overlap);

    assertEquals(trueSequence, mergedSequence.toString());
    checkAlignTags(r5Prefixes, mergedSequence, merger.allTags);
  }

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

      // Make sure the test is setup correctly.
      String srcSequence = DNAUtil.sequenceToDir(
          testcase.canonical_src, StrandsUtil.src(testcase.strands)).toString();
      String destSequence = DNAUtil.sequenceToDir(
          testcase.canonical_dest,
          StrandsUtil.dest(testcase.strands)).toString();

      String srcOverlap = srcSequence.substring(
          srcSequence.length() - overlap, srcSequence.length());
      String destOverlap = destSequence.substring(0, overlap);

      if (!srcOverlap.equals(destOverlap)) {
        fail("Test not setup correctly; sequences don't overlap.");
      }
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
      HashMap<String, String> r5Prefixes,
      Sequence canonicalMerged, List<R5Tag> aligned) {
    assertEquals(r5Prefixes.size(), aligned.size());
    for (R5Tag tag: aligned) {
      String prefix = R5TagUtil.prefixForTag(canonicalMerged, tag).toString();

      // The prefix returned from the merge sequence could be longer
      // than the original prefix, so we only compare the length of
      // the original prefix.
      assertTrue(r5Prefixes.containsKey(tag.getTag().toString()));
      String true_prefix = r5Prefixes.get(tag.getTag().toString());

      assertTrue(prefix.startsWith(true_prefix));
    }
  }

  protected static class NodesTestCase{
    // Chain of nodes to merge.
    private final ArrayList<EdgeTerminal> chain;
    private final HashMap<String, GraphNode> nodes;

    // The expected merged node.
    private GraphNode expectedNode;

    public GraphNode src;
    public GraphNode dest;

    //public int src_coverage_length;
    //public int dest_coverage_length;

    public SequenceTestCase sequence_info;

    public NodesTestCase() {
      chain = new ArrayList<EdgeTerminal>();
      nodes = new HashMap<String, GraphNode>();
    }

    public List<EdgeTerminal> getChain() {
      return chain;
    }

    public HashMap<String, GraphNode> getNodes() {
      return nodes;
    }

    public int getOverlap() {
      return sequence_info.overlap;
    }

    /**
     * Return the id to use for the merged node so that it matches
     * that assigned to the expected merged node.
     * @return
     */
    public String mergedId() {
      return "merged";
    }

    private static float computeExpectedCoverage(
        GraphNode src, GraphNode dest, int overlap) {
      int srcCoverageLength = src.getSequence().size() - overlap;
      int destCoverageLength = dest.getSequence().size() - overlap;

      float trueCoverage =
          srcCoverageLength * src.getCoverage() +
          destCoverageLength * dest.getCoverage();
      float denominator = srcCoverageLength + destCoverageLength;
      trueCoverage = trueCoverage / denominator;

      return trueCoverage;
    }

    public static NodesTestCase createMergeTestCase(
        SequenceTestCase sequence_case, Random generator) {
      NodesTestCase nodeCase = new NodesTestCase();
      nodeCase.sequence_info = sequence_case;

      nodeCase.src = createNode("src", sequence_case.canonical_src, generator);
      nodeCase.dest = createNode(
          "dest", sequence_case.canonical_dest, generator);

      nodeCase.src.getData().getR5Tags().addAll(sequence_case.src_r5tags);
      nodeCase.dest.getData().getR5Tags().addAll(sequence_case.dest_r5tags);

      // We need to remove any existing edges between the strands that will
      // be merged.
      {
        DNAStrand srcStrand = StrandsUtil.src(nodeCase.sequence_info.strands);
        for (EdgeTerminal terminal : nodeCase.src.getEdgeTerminalsSet(
            srcStrand, EdgeDirection.OUTGOING)) {
          nodeCase.src.removeNeighbor(terminal.nodeId);
        }
      }
      {
        DNAStrand destStrand = StrandsUtil.dest(nodeCase.sequence_info.strands);
        for (EdgeTerminal terminal : nodeCase.dest.getEdgeTerminalsSet(
            destStrand, EdgeDirection.INCOMING)) {
          nodeCase.dest.removeNeighbor(terminal.nodeId);
        }
      }

      // Set the coverage.
      nodeCase.src.setCoverage(generator.nextFloat() * 100);
      nodeCase.dest.setCoverage(generator.nextFloat() * 100);

      // Create the expected node.
      nodeCase.expectedNode =  new GraphNode();
      nodeCase.expectedNode.setNodeId(nodeCase.mergedId());
      {
        Sequence mergedSequence = new Sequence(
            sequence_case.merged_sequence, DNAAlphabetFactory.create());
        mergedSequence = DNAUtil.canonicalseq(mergedSequence);

        nodeCase.expectedNode.setSequence(mergedSequence);
      }

      nodeCase.expectedNode.setCoverage(computeExpectedCoverage(
          nodeCase.src, nodeCase.dest, sequence_case.overlap));

      // Copy the edges to be preserved before we do the merge.
      {
        DNAStrand mergedStrand = sequence_case.merged_strand;
        DNAStrand destStrand = StrandsUtil.dest(sequence_case.strands);
        List<EdgeTerminal> destTerminals =
            nodeCase.dest.getEdgeTerminals(destStrand, EdgeDirection.OUTGOING);
        for (EdgeTerminal terminal: destTerminals) {
          ArrayList<String> tags = new ArrayList<String>();

          for (CharSequence tag:
            nodeCase.dest.getTagsForEdge(destStrand, terminal)) {
            tags.add(tag.toString());
          }
          nodeCase.expectedNode.addOutgoingEdgeWithTags(
              mergedStrand, terminal, tags, tags.size());
        }

        DNAStrand srcStrand = StrandsUtil.src(sequence_case.strands);
        List<EdgeTerminal> srcTerminals =
            nodeCase.src.getEdgeTerminals(srcStrand, EdgeDirection.INCOMING);
        for (EdgeTerminal terminal: srcTerminals) {
          ArrayList<String> tags = new ArrayList<String>();
          // We need to flip the terminal because we can only get tags for
          // outgoing edges, so we need to get the outgoing edge representing
          // this incoming edge.

          for (CharSequence tag:
            nodeCase.src.getTagsForEdge(DNAStrandUtil.flip(srcStrand),
                terminal.flip())) {
            tags.add(tag.toString());
          }
          nodeCase.expectedNode.addIncomingEdgeWithTags(
              mergedStrand, terminal, tags, tags.size());
        }
      }

      // Add bidirectional edge to src between src and dest.
      {
        EdgeTerminal terminal = new EdgeTerminal(
            nodeCase.dest.getNodeId(), StrandsUtil.dest(sequence_case.strands));
        DNAStrand strand = StrandsUtil.src(sequence_case.strands);
        nodeCase.src.addOutgoingEdge(strand, terminal);
      }

      // Add bidirectional edge to dest between src and dest.
      {
        StrandsForEdge rcstrands = StrandsUtil.complement(sequence_case.strands);
        DNAStrand strand = DNAStrandUtil.flip(
            StrandsUtil.src(rcstrands));
        EdgeTerminal terminal = new EdgeTerminal(
            nodeCase.src.getNodeId(), StrandsUtil.dest(rcstrands));
        nodeCase.dest.addOutgoingEdge(strand, terminal);
      }

      nodeCase.chain.add(new EdgeTerminal(
          "src", StrandsUtil.src(sequence_case.strands)));
      nodeCase.chain.add(new EdgeTerminal(
          "dest", StrandsUtil.dest(sequence_case.strands)));

      nodeCase.nodes.put(nodeCase.src.getNodeId(), nodeCase.src);
      nodeCase.nodes.put(nodeCase.dest.getNodeId(), nodeCase.dest);


      return nodeCase;
    }

    public DNAStrand getMergedStrand() {
      return sequence_info.merged_strand;
    }
  }

  protected static GraphNode createNode(
      String nodeid, Sequence sequence, Random generator) {
    long kMaxThreads = 1000;
    // Create a node with some edges.
    GraphNode node = new GraphNode();
    node.getData().setNodeId(nodeid);
    node.setSequence(sequence);

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

    return node;
  }

  /**
   * Check if the expected node is correct.
   */
  protected void assertExpectedNode(NodesTestCase testCase, GraphNode node) {
    // We remove the R5Tags from the node before checking the node
    // because the R5Tags aren't set in the expected node.
    List<R5Tag> tags = node.getData().getR5Tags();
    node.getData().setR5Tags(new ArrayList<R5Tag>());
    assertTrue(testCase.expectedNode.equals(node));

    node.getData().setR5Tags(tags);
    // Check the tags.
    checkAlignTags(testCase.sequence_info.r5prefix, node.getSequence(), tags);
  }

  @Test
  public void testMergeNodes() {
    int ntrials = 20;
    for (int trial = 0; trial < ntrials; trial++) {
      SequenceTestCase sequence_testcase = SequenceTestCase.random(generator);

      NodesTestCase nodeTestcase = NodesTestCase.createMergeTestCase(
          sequence_testcase, generator);

      NodeMerger merger = new NodeMerger();

      // Merge the nodes.
      MergeResult result = merger.mergeNodes(
          nodeTestcase.mergedId(), nodeTestcase.getChain(),
          nodeTestcase.getNodes(), nodeTestcase.getOverlap());

      // Check the sequence is set correctly.
      Sequence merged_sequence = DNAUtil.sequenceToDir(
          result.node.getSequence(), result.strand);
      assertEquals(
          sequence_testcase.merged_sequence,
          merged_sequence.toString());

      assertExpectedNode(nodeTestcase, result.node);
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
      EdgeTerminal srcTerminal = graph.findEdgeTerminalForSequence("GGC");
      EdgeTerminal destTerminal = graph.findEdgeTerminalForSequence("GCC");

      ArrayList<EdgeTerminal> chain = new ArrayList<EdgeTerminal>();
      chain.add(srcTerminal);
      chain.add(destTerminal);

      NodeMerger merger = new NodeMerger();
      result = merger.mergeNodes("merged", chain, graph.getAllNodes(), K-1);
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

  @Test
  public void testMergeCycle() {
    // Test we correctly merge the cycle A->B>A.
    // This test covers a subgraph that we saw in the staph dataset. The
    // graph is  B->X->RC(Y)->X .
    // In this case the cycle is automatically broken at X because of
    // the incoming edge from B. This means we will try to merge X & Y
    // So the resulting graph should be B->{XY}.
    GraphNode branchNode = new GraphNode();
    branchNode.setNodeId("branch");
    branchNode.setSequence(new Sequence("GGAC", DNAAlphabetFactory.create()));

    GraphNode cycleStart = new GraphNode();
    cycleStart.setNodeId("cyclestart");
    cycleStart.setSequence(new Sequence("ACT", DNAAlphabetFactory.create()));

    GraphNode cycleEnd = new GraphNode();
    cycleEnd.setNodeId("cyclend");
    cycleEnd.setSequence(new Sequence("CTAC", DNAAlphabetFactory.create()));

    GraphUtil.addBidirectionalEdge(
        branchNode, DNAStrand.FORWARD, cycleStart, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        cycleStart, DNAStrand.FORWARD, cycleEnd, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        cycleEnd, DNAStrand.FORWARD, cycleStart, DNAStrand.FORWARD);

    ArrayList<EdgeTerminal> chain = new ArrayList<EdgeTerminal>();
    chain.add(new EdgeTerminal(cycleStart.getNodeId(), DNAStrand.FORWARD));
    chain.add(new EdgeTerminal(cycleEnd.getNodeId(), DNAStrand.FORWARD));

    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    nodes.put(branchNode.getNodeId(), branchNode);
    nodes.put(cycleStart.getNodeId(), cycleStart);
    nodes.put(cycleEnd.getNodeId(), cycleEnd);

    int K = 3;

    NodeMerger merger = new NodeMerger();

    MergeResult result =
        merger.mergeNodes("merged", chain, nodes, K - 1);

    GraphNode expectedNode = new GraphNode();
    expectedNode.setNodeId("merged");

    expectedNode.setSequence(
        new Sequence("ACTAC", DNAAlphabetFactory.create()));
    GraphUtil.addBidirectionalEdge(
        expectedNode, DNAStrand.FORWARD, expectedNode, DNAStrand.FORWARD);

    GraphNode expectedBranch = branchNode.clone();
    GraphUtil.addBidirectionalEdge(
        expectedBranch, DNAStrand.FORWARD, expectedNode, DNAStrand.FORWARD);

    assertEquals(expectedNode, result.node);
  }

  @Test
  public void testHairPin() {
    // The following graph was encountered with human chromosome 14 and revealed
    // a bug in the original merge code.
    // ...->A:F->B:R->C:F->C:R->B:F->A:R->...
    // The strands aren't particularly important. What's important is that
    // the chain consists of a bunch of nodes and there reverse complements.

    // Let A= ACT
    // B = CTG
    // C = TG CG
    GraphNode nodeA = new GraphNode();
    nodeA.setNodeId("A");
    nodeA.setSequence(new Sequence("ACT", DNAAlphabetFactory.create()));

    GraphNode nodeB = new GraphNode();
    nodeB.setNodeId("B");
    nodeB.setSequence(new Sequence("CTG", DNAAlphabetFactory.create()));

    GraphNode nodeC = new GraphNode();
    nodeC.setNodeId("C");
    nodeC.setSequence(new Sequence("TGCG", DNAAlphabetFactory.create()));

    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeB, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeB, DNAStrand.FORWARD, nodeC, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        nodeC, DNAStrand.FORWARD, nodeC, DNAStrand.REVERSE);
    GraphUtil.addBidirectionalEdge(
        nodeC, DNAStrand.REVERSE, nodeB, DNAStrand.REVERSE);
    GraphUtil.addBidirectionalEdge(
        nodeB, DNAStrand.REVERSE, nodeA, DNAStrand.REVERSE);

    ArrayList<EdgeTerminal> chain = new ArrayList<EdgeTerminal>();
    chain.add(new EdgeTerminal("A", DNAStrand.FORWARD));
    chain.add(new EdgeTerminal("B", DNAStrand.FORWARD));
    chain.add(new EdgeTerminal("C", DNAStrand.FORWARD));
    chain.add(new EdgeTerminal("C", DNAStrand.REVERSE));
    chain.add(new EdgeTerminal("B", DNAStrand.REVERSE));
    chain.add(new EdgeTerminal("A", DNAStrand.REVERSE));

    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    nodes.put(nodeA.getNodeId(), nodeA);
    nodes.put(nodeB.getNodeId(), nodeB);
    nodes.put(nodeC.getNodeId(), nodeC);

    GraphNode expectedNode = new GraphNode();
    expectedNode.setNodeId("merged");

    Sequence expectedSequence = new Sequence(
        "ACTGCGCAGT", DNAAlphabetFactory.create());
    expectedSequence = DNAUtil.canonicalseq(expectedSequence);
    expectedNode.setSequence(expectedSequence);

    NodeMerger merger = new NodeMerger();
    int overlap = 2;
    MergeResult result = merger.mergeNodes("merged", chain, nodes, overlap);

    assertTrue(expectedNode.equals(result.node));
  }

  @Test
  public void testConnectedStrands() {
    // This test is slightly different from mergeNodeWithRC because
    // we are testing NodeMerger.mergeConnectedSrands.
    // Suppose we have the graph Y->X->R(X)->Z
    // and we call mergeConnectedStrands on X, we want to verify this produces
    // the correct result. Most importantly we want to ensure that edge
    // strands are preserved and consistent.
    // The edge Y->X is represented as FF in node Y. The corresponding edge
    // RC(X)->RC(Y) is represented as RR in node R.
    // So after the merge the edge should be R(M)->R(Y) where M is the result
    // of merging X and R(X).
    // We also consider the graph.
    // Y->R(X)->X->Z.
    // In this case the edges in X are attached to the F strand.
    for (DNAStrand xStrand : DNAStrand.values()) {
      GraphNode node = new GraphNode();
      node.setNodeId("X");

      Sequence catSequence = new Sequence("CAT", DNAAlphabetFactory.create());
      if (xStrand == DNAStrand.REVERSE) {
        // Take the complete of the sequence so that the edge from Y will
        // be the R strand of X.
        node.setSequence(DNAUtil.reverseComplement(catSequence));
      } else {
        node.setSequence(catSequence);
      }

      GraphUtil.addBidirectionalEdge(
          node, xStrand, node, DNAStrandUtil.flip(xStrand));

      GraphNode nodeY = new GraphNode();
      nodeY.setNodeId("Y");
      nodeY.setSequence(new Sequence("TCA", DNAAlphabetFactory.create()));

      GraphUtil.addBidirectionalEdge(
          nodeY, DNAStrand.FORWARD, node, xStrand);

      GraphNode nodeZ = GraphTestUtil.createNode("Z", "TGG");
      GraphUtil.addBidirectionalEdge(
          node, DNAStrandUtil.flip(xStrand), nodeZ, DNAStrand.FORWARD);

      GraphNode expected = new GraphNode();
      expected.setNodeId(node.getNodeId());
      expected.setSequence(new Sequence("CATG", DNAAlphabetFactory.create()));

      // The merged graph is Y,R(Z)->[X,R(X)] -> R(Y),Z
      // When we merge the nodes X and R(X) we want to preserve the incoming
      // edge to X and the outgoing edge to R(X). However, these incoming
      // outgoing edges should only be represented once in the merged node
      // as [X, R(X)] ->R(Y), Z. Furthermore, while both strands of the merged
      // node are the same, the strand used should be consistent with the
      // corresponding strand in nodes Y and Z for these edges.
      expected.addIncomingEdge(
          xStrand, new EdgeTerminal("Y", DNAStrand.FORWARD));
      expected.addOutgoingEdge(
          DNAStrandUtil.flip(xStrand),
          new EdgeTerminal("Z", DNAStrand.FORWARD));

      NodeMerger merger = new NodeMerger();
      GraphNode merged = merger.mergeConnectedStrands(node, 2);
      NodeDiff diff = expected.equalsWithInfo(merged);
      assertEquals(NodeDiff.NONE, diff);

      // Make sure the new graph is valid and consistent with the other
      // nodes.
      HashMap<String, GraphNode> newGraph = new HashMap<String, GraphNode>();
      for (GraphNode n : Arrays.asList(merged, nodeY, nodeZ)) {
        newGraph.put(n.getNodeId(), n);
      }
      List<GraphError> errors = GraphUtil.validateGraph(newGraph, 3);
      assertEquals(0, errors.size());
    }
  }

  @Test
  public void testConnectedStrandsNotMergeable() {
    // We test NodeMerger.mergeConnectedSrands doesn't merge the strands
    // when the strands can't be merged.
    // Suppose we have the graph X->R(X),A
    // and we call mergeConnectedStrands on X, no merge should happen
    // because X has outdegree 2.
    for (DNAStrand xStrand : DNAStrand.values()) {
      GraphNode node = new GraphNode();
      node.setNodeId("X");

      Sequence catSequence = new Sequence("CAT", DNAAlphabetFactory.create());
      if (xStrand == DNAStrand.REVERSE) {
        // Take the complete of the sequence so that the edge from Y will
        // be the R strand of X.
        node.setSequence(DNAUtil.reverseComplement(catSequence));
      } else {
        node.setSequence(catSequence);
      }

      GraphUtil.addBidirectionalEdge(
          node, xStrand, node, DNAStrandUtil.flip(xStrand));

      GraphNode nodeA = GraphTestUtil.createNode("A", "ATGG");

      GraphUtil.addBidirectionalEdge(
          node, xStrand, nodeA, DNAStrand.FORWARD);

      NodeMerger merger = new NodeMerger();
      GraphNode merged = merger.mergeConnectedStrands(node, 2);
      assertEquals(null, merged);
    }
  }
}
