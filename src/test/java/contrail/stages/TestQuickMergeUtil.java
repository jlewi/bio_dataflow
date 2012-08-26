package contrail.stages;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphError;
import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.util.ListUtil;
import contrail.util.Tuple;

// Extend QuickMergeUtil so we can test protected methods
public class TestQuickMergeUtil extends QuickMergeUtil {
  // Random number generator.
  private Random generator;
  @Before
  public void setUp() {
    // Create a random generator.
    generator = new Random();
  }

  @Test
  public void testcheckIncomingEdgesAreInMemory() {
    final int K = 3;
    // Create a simple graph.
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("CTG", "TGA", K - 1);
    graph.addEdge("ATG", "TGA", K - 1);


    assertTrue(checkIncomingEdgesAreInMemory(
        graph.getAllNodes(), graph.findEdgeTerminalForSequence("TGA")));

    // Remove one of the edges from memory and check it returns false.
    Hashtable<String, GraphNode> nodes = new Hashtable<String, GraphNode>();
    nodes.putAll(graph.getAllNodes());
    nodes.remove(graph.findEdgeTerminalForSequence("ATG").nodeId);

    assertFalse(checkIncomingEdgesAreInMemory(
        nodes, graph.findEdgeTerminalForSequence("TGA")));
  }

  /**
   * Contains information about a chain constructed for some test.
   */
  private static class ChainNode {
    // GraphNodes in the order they are connected.
    GraphNode graph_node;

    // The direction for the dna in this chain.
    DNAStrand dna_direction;

    public String toString() {
      return graph_node.getNodeId() + ":" + dna_direction.toString();
    }
  }

  /**
   * Construct a linear chain
   * @param length
   * @return
   */
  private ArrayList<ChainNode> constructChain(int length, int K) {
    // Note if the chain has repeated KMers each repeat will be assigned
    // to a unique node so we can still reconstruct the chain perfectly. This
    // is different from what would happen when using BuildGraph.
    String sequence = AlphabetUtil.randomString(
        generator, length, DNAAlphabetFactory.create());

    return constructChainForSequence(sequence, K);
  }

  /**
   * Construct a linear chain from the given sequence using KMers of length K.
   * @param length
   * @return
   */
  private ArrayList<ChainNode> constructChainForSequence(
      String full_sequence, int K) {
    ArrayList<ChainNode> chain = new ArrayList<ChainNode>();

    // Construct the nodes.
    for (int pos = 0; pos <= full_sequence.length() - K; pos++) {
      // Construct a graph node.
      GraphNode node = new GraphNode();
      node.setNodeId("node_" + pos);

      Sequence sequence = new Sequence(
          full_sequence.substring(pos, pos + K), DNAAlphabetFactory.create());
      node.setSequence(DNAUtil.canonicalseq(sequence));
      ChainNode chain_node = new ChainNode();
      chain_node.graph_node = node;
      chain_node.dna_direction = DNAUtil.canonicaldir(sequence);
      chain.add(chain_node);
    }

    // Now connect the nodes.
    // TODO(jlewi): We should add incoming and outgoing edges to the first
    // and last node so that we test that walker stops when the node isn't
    // in memory.
    for (int pos = 0; pos < chain.size(); pos++) {
      // Add the outgoing edge.
      if (pos + 1 < chain.size()) {
        ChainNode src = chain.get(pos);
        ChainNode dest = chain.get(pos + 1);
        EdgeTerminal terminal = new EdgeTerminal(
            dest.graph_node.getNodeId(),
            dest.dna_direction);
        src.graph_node.addOutgoingEdge(src.dna_direction, terminal);
      }

      // Add the incoming edge.
      if (pos > 0) {
        ChainNode src = chain.get(pos);
        ChainNode dest = chain.get(pos - 1);
        EdgeTerminal terminal = new EdgeTerminal(
            dest.graph_node.getNodeId(),
            dest.dna_direction);
        src.graph_node.addIncomingEdge(src.dna_direction, terminal);

      }
    }
    return chain;
  }

  /**
   * Find the terminals corresponding to the start and end of the
   * chain assuming we start the search at search_start
   * @param chain_nodes
   * @param nodes
   * @param search_start
   * @return
   */
  private Tuple<EdgeTerminal, EdgeTerminal> findChainEnds(
      ArrayList<ChainNode> chain_nodes,
      int search_start) {
    EdgeTerminal start_terminal;
    EdgeTerminal end_terminal;
    if (chain_nodes.get(search_start).dna_direction == DNAStrand.FORWARD) {
      int start_pos = 0;
      int end_pos = chain_nodes.size() -1;
      start_terminal = new EdgeTerminal(
          chain_nodes.get(start_pos).graph_node.getNodeId(),
          chain_nodes.get(start_pos).dna_direction);
      end_terminal = new EdgeTerminal(
          chain_nodes.get(end_pos).graph_node.getNodeId(),
          chain_nodes.get(end_pos).dna_direction);
    } else {
      int start_pos = chain_nodes.size() -1;
      int end_pos = 0;
      start_terminal = new EdgeTerminal(
          chain_nodes.get(start_pos).graph_node.getNodeId(),
          chain_nodes.get(start_pos).dna_direction);
      end_terminal = new EdgeTerminal(
          chain_nodes.get(end_pos).graph_node.getNodeId(),
          chain_nodes.get(end_pos).dna_direction);

      // Flip the nodes because we are on the opposite strand
      // of the one we constructed the chain for.
      start_terminal = start_terminal.flip();
      end_terminal = end_terminal.flip();
    }
    return new Tuple<EdgeTerminal, EdgeTerminal>(start_terminal, end_terminal);
  }

  /**
   * Run a trial to test findNodesTomerge
   *
   * This function assumes that the chain of nodes forms a linear chain,
   * so the result of finding the nodes should always be the nodes at
   * the ends of the chain.
   *
   * @param chain_nodes: Chain of nodes.
   * @param search_start: Which node in the chain to start on.
   */
  private void runFindNodesToMergeTrial(
      ArrayList<ChainNode> chain_nodes, HashMap<String, GraphNode> nodes,
      int search_start) {
    // Determine which chain links should correspond to the start and end
    // of the merge.
    EdgeTerminal start_terminal;
    EdgeTerminal end_terminal;

    Tuple<EdgeTerminal, EdgeTerminal> chain_ends= findChainEnds(
        chain_nodes, search_start);

    start_terminal = chain_ends.first;
    end_terminal = chain_ends.second;
    QuickMergeUtil.NodesToMerge merge_info = QuickMergeUtil.findNodesToMerge(
        nodes, chain_nodes.get(search_start).graph_node);

    assertEquals(start_terminal, merge_info.start_terminal);
    assertEquals(end_terminal, merge_info.end_terminal);
  }

  @Test
  public void testfindNodesToMerge() {
    ArrayList<ChainNode> chain_nodes = constructChain(5, 3);
    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    for (ChainNode link: chain_nodes) {
      nodes.put(link.graph_node.getNodeId(), link.graph_node);
    }
    {
      // Start in the middle.
      int middle = 2;
      runFindNodesToMergeTrial(chain_nodes, nodes, middle);
    }
    {
      // Start at the head.
      int start_search = 0;
      runFindNodesToMergeTrial(chain_nodes, nodes, start_search);
    }
    {
      // Start at the end.
      int start_search = chain_nodes.size() - 1;
      runFindNodesToMergeTrial(chain_nodes, nodes, start_search);
    }
    {
      int start_search = 2;
      // Add an outgoing edge to the end of the chain.
      // Make sure that we still return the start and
      // end of the chain as the limits of the merge.
      Tuple<EdgeTerminal, EdgeTerminal> chain_ends = findChainEnds(
          chain_nodes, start_search);
      // Add an outgoing edge to the end.
      GraphNode tail_node = new GraphNode();
      tail_node.getData().setNodeId("tail_end");
      EdgeTerminal terminal = new EdgeTerminal(
          tail_node.getNodeId(), chain_ends.second.strand);
      nodes.get(chain_ends.second.nodeId).addOutgoingEdge(
          chain_ends.second.strand, terminal);
    }
  }

  @Test
  public void testfindNodesToMergeCases() {
    // Test various specific cases to make sure the behavior is correct.
    final int K = 3;
    // Create a simple graph.
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString("CGCAAT", K);

    // Add some wings to the edges.
    graph.addEdge("AAT", "ATA", K - 1);
    graph.addEdge("AAT", "ATG", K - 1);

    graph.addEdge("TCG", "CGC", K - 1);
    graph.addEdge("ACG", "CGC", K - 1);

    {
      EdgeTerminal start_terminal = graph.findEdgeTerminalForSequence("GCA");
      GraphNode start_node = graph.getNode(start_terminal.nodeId);
      NodesToMerge info = findNodesToMerge(graph.getAllNodes(), start_node);

      // Note: The merge always starts on the forward strand of the start
      // terminal.
      assertEquals(
          info.start_terminal, graph.findEdgeTerminalForSequence("CGC"));
      assertEquals(
          info.end_terminal, graph.findEdgeTerminalForSequence("AAT"));

      HashSet<String> expected_visited = new HashSet<String>();
      expected_visited.add(graph.findEdgeTerminalForSequence("ATT").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("TTG").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("TGC").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("GCG").nodeId);
      assertEquals(expected_visited, info.nodeids_visited);
    }

    {
      // Repeat the test but this time remove the nodes ACG and ATG
      // from memory. As a result, we shouldn't be able to include the last
      // nodes in memory.
      Hashtable<String, GraphNode> nodes = new Hashtable<String, GraphNode>();
      nodes.putAll(graph.getAllNodes());
      nodes.remove(graph.findEdgeTerminalForSequence("ACG").nodeId);
      nodes.remove(graph.findEdgeTerminalForSequence("ATG").nodeId);

      EdgeTerminal start_terminal = graph.findEdgeTerminalForSequence("CGC");
      GraphNode start_node = graph.getNode(start_terminal.nodeId);
      NodesToMerge info = findNodesToMerge(nodes, start_node);

      assertEquals(
          info.start_terminal, graph.findEdgeTerminalForSequence("GCA"));
      assertEquals(
          info.end_terminal, graph.findEdgeTerminalForSequence("CAA"));

      HashSet<String> expected_visited = new HashSet<String>();
      expected_visited.add(graph.findEdgeTerminalForSequence("CGC").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("GCA").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("CAA").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("AAT").nodeId);
      assertEquals(expected_visited, info.nodeids_visited);

    }
  }

  @Test
  public void testfindNodesToMergeNoMerge() {
    // We test the graph
    // A-> C->D->E->F
    // B->C
    // E->G
    // So in principle we can merge C-D-E. However, in this test case
    // B and G won't be in memory so we can't do the merge because we can't
    // move edges B->C and RC(G)->RC(E). We could still do a single merge
    // (i.e C & D or D & E) provided we assigned the merged node an id and
    // strand such that the edges wouldn't have to change. The code, however,
    // opts for the simpler case of not doing any merges.
    final int K = 3;
    // Create a simple graph.
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString("CGCAA", K);

    // Add some wings to the edges.
    graph.addEdge("TCG", "CGC", K - 1);
    graph.addEdge("ACG", "CGC", K - 1);

    graph.addEdge("CAA", "AAA", K - 1);
    graph.addEdge("CAA", "AAT", K - 1);
    {
      // When the wings are in memory we can do the merge.
      EdgeTerminal start_terminal = graph.findEdgeTerminalForSequence("GCA");
      GraphNode start_node = graph.getNode(start_terminal.nodeId);
      NodesToMerge info = findNodesToMerge(graph.getAllNodes(), start_node);

      // Note: The merge always starts on the forward strand of the start
      // terminal.
      assertEquals(
          info.start_terminal, graph.findEdgeTerminalForSequence("CGC"));
      assertEquals(
          info.end_terminal, graph.findEdgeTerminalForSequence("CAA"));

      HashSet<String> expected_visited = new HashSet<String>();
      expected_visited.add(graph.findEdgeTerminalForSequence("CGC").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("GCA").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("CAA").nodeId);
      assertEquals(expected_visited, info.nodeids_visited);
    }

    {
      // Repeat the test but this time remove the nodes ACG and AAA
      // from memory. As a result, we shouldn't be able to include the last
      // nodes in memory which means we can't do any merges..
      Hashtable<String, GraphNode> nodes = new Hashtable<String, GraphNode>();
      nodes.putAll(graph.getAllNodes());
      nodes.remove(graph.findEdgeTerminalForSequence("ACG").nodeId);
      nodes.remove(graph.findEdgeTerminalForSequence("AAA").nodeId);

      EdgeTerminal start_terminal = graph.findEdgeTerminalForSequence("CGC");
      GraphNode start_node = graph.getNode(start_terminal.nodeId);
      NodesToMerge info = findNodesToMerge(nodes, start_node);

      assertEquals(info.start_terminal, null);
      assertEquals(info.end_terminal, null);

      HashSet<String> expected_visited = new HashSet<String>();
      expected_visited.add(graph.findEdgeTerminalForSequence("CGC").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("GCA").nodeId);
      expected_visited.add(graph.findEdgeTerminalForSequence("CAA").nodeId);
      assertEquals(expected_visited, info.nodeids_visited);
    }
  }
  @Test
  public void testMergeLinearChain() {
    for (int trial = 0; trial < 10; trial++) {
      // Even though we have repeats the merge still work, because
      // the way we construct the chain, we have one node for each instance
      // rather than representing all instances of the same KMer using a single
      // node.
      int K = generator.nextInt(20) + 3;
      int length = generator.nextInt(100) + K;
      String full_sequence = AlphabetUtil.randomString(
          generator, length, DNAAlphabetFactory.create());

      int overlap = K - 1;
      ArrayList<ChainNode> chain_nodes =
          constructChainForSequence(full_sequence, K);
      HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
      for (ChainNode link: chain_nodes) {
        nodes.put(link.graph_node.getNodeId(), link.graph_node);
      }

      // Do the merge.
      int start_search = generator.nextInt(chain_nodes.size());
      QuickMergeUtil.NodesToMerge merge_info =
          QuickMergeUtil.findNodesToMerge(
               nodes, chain_nodes.get(start_search).graph_node);
      QuickMergeUtil.ChainMergeResult result =
          QuickMergeUtil.mergeLinearChain(nodes, merge_info, overlap);

      Sequence full_canonical = new Sequence(
          full_sequence, DNAAlphabetFactory.create());

      full_canonical = DNAUtil.canonicalseq(full_canonical);

      // Check the sequence equals the original sequence.
      assertEquals(full_canonical, result.merged_node.getSequence());

      Set<String> expected_merged_ids = nodes.keySet();
      assertEquals(expected_merged_ids, result.merged_nodeids);
    }
  }

  @Test
  public void testMergeLinearChainCases() {
    // Some special cases.
    {
      final int K = 3;
      SimpleGraphBuilder graph = new SimpleGraphBuilder();
      graph.addKMersForString("CGCA", K);
      graph.addEdge("ACG", "CGC", K - 1);
      graph.addEdge("TCG", "CGC", K - 1);
      graph.addEdge("GCA", "CAC", K - 1);
      graph.addEdge("GCA", "CAA", K - 1);

      {
        // Consider a short chain, i.e no interior nodes.
        QuickMergeUtil.NodesToMerge merge_info =
            QuickMergeUtil.findNodesToMerge(
                 graph.getAllNodes(), graph.getNode("CGC"));

        assertEquals(graph.findEdgeTerminalForSequence("CGC"),
                     merge_info.start_terminal);
        assertEquals(graph.findEdgeTerminalForSequence("GCA"),
            merge_info.end_terminal);

        QuickMergeUtil.ChainMergeResult result = QuickMergeUtil.mergeLinearChain(
            graph.getAllNodes(), merge_info, K - 1);

        assertEquals(
            "CGCA",
            result.merged_node.getSequence().toString());

        HashSet<String> expected_visited = new HashSet<String>();
        expected_visited.add("CGC");
        expected_visited.add("GCA");
        assertEquals(expected_visited, merge_info.nodeids_visited);
        assertEquals(expected_visited, result.merged_nodeids);
      }

      {
        // Repeat the test but remove one of the incoming edges from memory
        Hashtable<String, GraphNode> nodes = new Hashtable<String, GraphNode>();
        nodes.putAll(graph.getAllNodes());
        nodes.remove(graph.findEdgeTerminalForSequence("ACG").nodeId);
        QuickMergeUtil.NodesToMerge merge_info =
            QuickMergeUtil.findNodesToMerge(
                 nodes, graph.getNode("CGC"));

        // We shouldn't be able to do a merge.
        assertEquals(null, merge_info.start_terminal);
        assertEquals(null, merge_info.end_terminal);
      }
    }
  }

  @Test
  public void testMergeRepeated() {
    // Consider the special case where we have a chain in which
    // some sequence and its reverse complement appears.
    // Note: if a->y->b ...-c>RC(y)->d
    // Then a->y implies RC(y)->RC(A)
    // which implies either
    // 1) d = RC(a)
    // 2) d != RC(A) then RC(Y) has two outgoing edges
    // RC(y)->d, and RC(y)->RC(a)  in this case we can't merge RC(y) with d.
    // Lets test case 1 where d = RC(A)
    // e.g: ATCGAT with K = 3 gives rise to
    // ATC->TCG->CGA->GAT
    // ATG = RC(GAT)
    // TCG = RC(CGA)
    final int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    String true_sequence_str = "ATCGAT";
    graph.addKMersForString(true_sequence_str, K);
    Sequence true_canonical = new Sequence(
        true_sequence_str, DNAAlphabetFactory.create());
    DNAStrand true_strand = DNAUtil.canonicaldir(true_canonical);
    true_canonical = DNAUtil.canonicalseq(true_canonical);

    // Give GAT incoming degree 2 and ATC outdegree 2
    // this way we can verify that these edges are preserved.
    graph.addEdge("CAT", "ATC", 2);
    graph.addEdge("TAT", "ATC", 2);

    graph.addEdge("GAT", "ATA", 2);
    graph.addEdge("GAT", "ATT", 2);

    {
      // TODO(jlewi): Verify test is setup correctly i.e we have a KMer
      // and its reverse complement

      // Minimial check that the graph is correct.
      assertEquals(5, graph.getAllNodes().size());
    }

    Hashtable<String, GraphNode> nodes = new Hashtable<String, GraphNode> ();
    nodes.putAll(graph.getAllNodes());

    GraphNode start_node;
    {
      int start = generator.nextInt(true_sequence_str.length() - K + 1);
      EdgeTerminal terminal = graph.findEdgeTerminalForSequence(
          true_sequence_str.substring(start, start + K));
      start_node = graph.getNode(terminal.nodeId);
    }
    NodesToMerge nodes_to_merge = QuickMergeUtil.findNodesToMerge(
        graph.getAllNodes(), start_node);

    ChainMergeResult result = QuickMergeUtil.mergeLinearChain(
        nodes, nodes_to_merge, K - 1);

    // Check the merged sequence is correct.
    assertEquals(true_canonical, result.merged_node.getSequence());

    HashSet<String> seen_nodeids = new HashSet<String>();
    seen_nodeids.add("ATC");
    seen_nodeids.add("CGA");
    assertEquals(
        seen_nodeids, result.merged_nodeids);

    // Check that the edges at the start and end are correct.
    {
      List<EdgeTerminal> expected_outgoing = new ArrayList<EdgeTerminal>();

      // Add the outgoing edges from GAT.
      expected_outgoing.add(graph.findEdgeTerminalForSequence("ATA"));
      expected_outgoing.add(graph.findEdgeTerminalForSequence("ATT"));

      // Add the reverse complement for the incoming edges to ATC.
      expected_outgoing.add(graph.findEdgeTerminalForSequence("ATG"));

      List<EdgeTerminal> outgoing = result.merged_node.getEdgeTerminals(
          true_strand, EdgeDirection.OUTGOING);
      assertTrue(ListUtil.listsAreEqual(expected_outgoing, outgoing));
    }
    {
      List<EdgeTerminal> expected_incoming = new ArrayList<EdgeTerminal>();

      // Add the incoming edges to ATC.
      expected_incoming.add(graph.findEdgeTerminalForSequence("CAT"));
      expected_incoming.add(graph.findEdgeTerminalForSequence("TAT"));

      // Add the reverse complement for the outgoing edges from GAT.
      expected_incoming.add(graph.findEdgeTerminalForSequence("AAT"));

      List<EdgeTerminal> incoming = result.merged_node.getEdgeTerminals(
          true_strand, EdgeDirection.INCOMING);
      assertTrue(ListUtil.listsAreEqual(expected_incoming, incoming));
    }
  }

  @Test
  public void testBreakCycle() {
    // Consider the special case where we have a cycle
    final int K = 3;
    String true_sequence_str = "ATCGCATC";

    String true_merged = "ATCGCAT";

    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString(true_sequence_str, K);

    // Get the KMer corresponding to the start
    String start_kmer = true_merged.substring(0, K);

    EdgeTerminal start_terminal = graph.findEdgeTerminalForSequence(start_kmer);
    GraphNode start_node = graph.getAllNodes().get(start_terminal.nodeId);
    NodesToMerge nodes_to_merge = QuickMergeUtil.findNodesToMerge(
        graph.getAllNodes(), start_node);

    Sequence true_canonical;
    {
      Sequence true_sequence = new Sequence(
          true_merged, DNAAlphabetFactory.create());
      //DNAStrand true_strand = DNAUtil.canonicaldir(true_sequence);
      true_canonical = DNAUtil.canonicalseq(true_sequence);
    }

    String last_kmer = true_merged.substring(
        true_merged.length() - K, true_merged.length());

    EdgeTerminal expected_start =
        graph.findEdgeTerminalForSequence(start_kmer);
    EdgeTerminal expected_end =
        graph.findEdgeTerminalForSequence(last_kmer);

    assertEquals(nodes_to_merge.start_terminal, expected_start);
    assertEquals(nodes_to_merge.end_terminal, expected_end);
    assertEquals(
        graph.getAllNodes().keySet(), nodes_to_merge.nodeids_visited);

    ChainMergeResult result = QuickMergeUtil.mergeLinearChain(
        graph.getAllNodes(), nodes_to_merge, K - 1);

    // Check the merged sequence is correct.
    assertEquals(true_canonical, result.merged_node.getSequence());

    // Check the cycle is closed i.e it has incoming/outgoing edges to
    // itself.
    {
      // Since its a cycle, there should be an outgoing edge to itself.
      List<EdgeTerminal> expected_edges = new ArrayList<EdgeTerminal>();
      expected_edges.add(
          new EdgeTerminal(result.merged_node.getNodeId(), DNAStrand.FORWARD));

      List<EdgeTerminal> edges = result.merged_node.getEdgeTerminals(
          DNAStrand.FORWARD, EdgeDirection.OUTGOING);
      assertTrue(ListUtil.listsAreEqual(expected_edges, edges));
    }

    {
      // Since its a cycle, there should be an incoming edge to itself.
      List<EdgeTerminal> expected_edges = new ArrayList<EdgeTerminal>();
      expected_edges.add(
          new EdgeTerminal(result.merged_node.getNodeId(), DNAStrand.FORWARD));

      List<EdgeTerminal> edges = result.merged_node.getEdgeTerminals(
          DNAStrand.FORWARD, EdgeDirection.INCOMING);
      assertTrue(ListUtil.listsAreEqual(expected_edges, edges));
    }
  }

  @Test
  public void testSelfCycle() {
    // The graph in this test case is
    // A->X->R(X)->R(A) which gets merged into
    // A->X'->R(A).
    // The tricky part in this graph is making sure the edges in A are
    // properly updated. In the original graph A has a single outgoing edge.
    // but in the merged graph A has two edges A->X' and A->R(X').
    final int K = 5;
    GraphNode selfNode = new GraphNode();
    selfNode.setNodeId("X");
    selfNode.setSequence(new Sequence("ACTAG", DNAAlphabetFactory.create()));
    {
      Sequence complement = DNAUtil.reverseComplement(selfNode.getSequence());
      if (!selfNode.getSequence().subSequence(1, K).equals(
          complement.subSequence(0, K-1))) {
        fail("Test isn't setup correctly");
      }
    }
    GraphUtil.addBidirectionalEdge(
        selfNode, DNAStrand.FORWARD, selfNode, DNAStrand.REVERSE);

    GraphNode border = new GraphNode();
    border.setNodeId("A");
    border.setSequence(new Sequence("AACTA", DNAAlphabetFactory.create()));
    GraphUtil.addBidirectionalEdge(
        border, DNAStrand.FORWARD, selfNode, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        border, DNAStrand.FORWARD, selfNode, DNAStrand.FORWARD);

    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    nodes.put(selfNode.getNodeId(), selfNode);
    nodes.put(border.getNodeId(), border);

    {
      List<GraphError> graphErrors = GraphUtil.validateGraph(nodes, K);
      if (graphErrors.size() > 0) {
        fail("Test graph isn't valid.");
      }
    }

    // The expected graph.
    // A->X'->R(A)
    GraphNode expectedSelf = new GraphNode();
    expectedSelf.setNodeId("X");
    expectedSelf.setSequence(
        new Sequence("ACTAGT", DNAAlphabetFactory.create()));

    GraphNode expectedBorder = new GraphNode();
    expectedBorder.setNodeId("A");
    expectedBorder.setSequence(
        new Sequence("AACTA", DNAAlphabetFactory.create()));

    GraphUtil.addBidirectionalEdge(
        expectedBorder, DNAStrand.FORWARD, expectedSelf, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        expectedSelf, DNAStrand.FORWARD, expectedBorder, DNAStrand.REVERSE);

    {
      HashMap<String, GraphNode> expectedNodes =
          new HashMap<String, GraphNode>();
      expectedNodes.put(expectedSelf.getNodeId(), expectedSelf);
      expectedNodes.put(expectedBorder.getNodeId(), expectedBorder);
      List<GraphError> graphErrors = GraphUtil.validateGraph(expectedNodes, K);
      if (graphErrors.size() > 0) {
        fail("Expected graph isn't valid.");
      }
    }

    // Merge X and R(X)
    NodesToMerge nodesToMerge = new NodesToMerge();
    nodesToMerge.direction = EdgeDirection.OUTGOING;
    nodesToMerge.start_terminal = new EdgeTerminal(
        selfNode.getNodeId(), DNAStrand.FORWARD);
    nodesToMerge.end_terminal = new EdgeTerminal(
        selfNode.getNodeId(), DNAStrand.REVERSE);
    nodesToMerge.hit_cycle = false;
    ChainMergeResult result =
        QuickMergeUtil.mergeLinearChain(nodes, nodesToMerge, K - 1);

    HashMap<String, GraphNode> mergedGraph = new HashMap<String, GraphNode>();
    mergedGraph.putAll(nodes);
    for (String mergedId: result.merged_nodeids) {
      mergedGraph.remove(mergedId);
    }
    mergedGraph.put(result.merged_node.getNodeId(), result.merged_node);

    // Check that the node was properly merged.
    assertEquals(expectedSelf.getData(), result.merged_node.getData());
    assertEquals(expectedBorder.getData(), border.getData());
    List<GraphError> graphErrors = GraphUtil.validateGraph(mergedGraph, K);
    assertEquals(0, graphErrors.size());
  }
}
