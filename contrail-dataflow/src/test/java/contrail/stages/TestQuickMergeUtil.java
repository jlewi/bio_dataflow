package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
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
import contrail.graph.GraphNode.NodeDiff;
import contrail.graph.GraphTestUtil;
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

    @Override
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
  public void testfindNodesToMergeImperfectCycle() {
    // We test the graph
    // A->B->C->A
    // A->D
    // We have a cycle A->B->C but it's not a "perfect" cycle because
    // A has an outgoing edge which isn't part of the cycle.
    // Unlike a perfect cycle we can't break the cycle at any node,
    // we need to merge it such that A is the last node in the merged chain.
    // So the merged graph will be:
    // BCA->BCA
    // BCA->D
    // Create a simple graph.
    GraphNode nodeA = new GraphNode();
    nodeA.setNodeId("A");
    nodeA.setSequence(new Sequence("ACT", DNAAlphabetFactory.create()));

    GraphNode nodeB = new GraphNode();
    nodeB.setNodeId("B");
    nodeB.setSequence(new Sequence("CTG", DNAAlphabetFactory.create()));

    GraphNode nodeC = new GraphNode();
    nodeC.setNodeId("C");
    nodeC.setSequence(new Sequence("TGAC", DNAAlphabetFactory.create()));

    GraphNode nodeD = new GraphNode();
    nodeD.setNodeId("D");
    nodeD.setSequence(new Sequence("CTT", DNAAlphabetFactory.create()));

    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeB, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        nodeB, DNAStrand.FORWARD, nodeC, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        nodeC, DNAStrand.FORWARD, nodeA, DNAStrand.FORWARD);

    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeD, DNAStrand.FORWARD);

    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    nodes.put(nodeA.getNodeId(), nodeA);
    nodes.put(nodeB.getNodeId(), nodeB);
    nodes.put(nodeC.getNodeId(), nodeC);
    nodes.put(nodeD.getNodeId(), nodeD);
    QuickMergeUtil.NodesToMerge nodesToMerge =
        QuickMergeUtil.findNodesToMerge(nodes, nodeA);

    assertEquals(
        new EdgeTerminal(nodeB.getNodeId(), DNAStrand.FORWARD),
        nodesToMerge.start_terminal);

    assertEquals(
        new EdgeTerminal(nodeA.getNodeId(), DNAStrand.FORWARD),
        nodesToMerge.end_terminal);

    // Imperfect cycles shouldn't be considered cycles at this
    assertFalse(nodesToMerge.hit_cycle);
  }

  @Test
  public void testMergeLinearChain() {
    for (int trial = 0; trial < 10; trial++) {
      // Even though we have repeats the merge still work, because
      // the way we construct the chain, we have one node for each instance
      // rather than representing all instances of the same KMer using a single
      // node.
      int K = generator.nextInt(20) + 3;
      // The length must be at least K + 1 so we have at least two nodes.
      int length = generator.nextInt(100) + 1 + K;
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
    // ATC = RC(GAT)
    // TCG = RC(CGA)

    final int K = 3;
    GraphNode nodeATC = GraphTestUtil.createNode("ATC", "ATC");
    GraphNode nodeTCG = GraphTestUtil.createNode("TCG", "TCG");

    GraphUtil.addBidirectionalEdge(
        nodeATC, DNAStrand.FORWARD, nodeTCG, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeTCG, DNAStrand.FORWARD, nodeTCG, DNAStrand.REVERSE);
    GraphUtil.addBidirectionalEdge(
        nodeTCG, DNAStrand.REVERSE, nodeATC, DNAStrand.REVERSE);

    // Give GAT incoming degree 2 and ATC outdegree 2
    // So the graph is.
    // {CAT,TAT}-> ATC->TCG->CGA->GAT->{ATA, ATT}
    // But since ATC=RC(GAT). The actual graph is
    //{CAT,TAT, RC(ATA), RC(ATT)}-> ATC->TCG->CGA->GAT->{RC(CAT), RC(TAT), ATA, ATT}
    GraphNode nodeCAT = GraphTestUtil.createNode("CAT", "CAT");
    GraphNode nodeTAT = GraphTestUtil.createNode("TAT", "TAT");
    GraphUtil.addBidirectionalEdge(
        nodeCAT, DNAStrand.FORWARD, nodeATC, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeTAT, DNAStrand.FORWARD, nodeATC, DNAStrand.FORWARD);

    GraphNode nodeATA = GraphTestUtil.createNode("ATA", "ATA");
    GraphNode nodeATT = GraphTestUtil.createNode("ATT", "ATT");
    GraphUtil.addBidirectionalEdge(
        nodeATC, DNAStrand.REVERSE, nodeATA, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeATC, DNAStrand.REVERSE, nodeATT, DNAStrand.FORWARD);

    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    for (GraphNode node : Arrays.asList(
        nodeATC, nodeTCG, nodeCAT, nodeTAT, nodeATA, nodeATT)) {
      // Clone the node since merging the results will modify
      // the non-merged nodes in place.
      nodes.put(node.getNodeId(), node.clone());
    }

    NodesToMerge nodes_to_merge = QuickMergeUtil.findNodesToMerge(
        nodes, nodeATC);

    ChainMergeResult result = QuickMergeUtil.mergeLinearChain(
        nodes, nodes_to_merge, K - 1);

    // Check the merged node is correct.
    GraphNode expected = GraphTestUtil.createNode("ATC", "ATCGAT");
    expected.addIncomingEdge(
        DNAStrand.FORWARD,
        new EdgeTerminal(nodeCAT.getNodeId(), DNAStrand.FORWARD));
    expected.addIncomingEdge(
        DNAStrand.FORWARD,
        new EdgeTerminal(nodeTAT.getNodeId(), DNAStrand.FORWARD));

    // Since the graph is A->...->RC(A)
    // All incoming edges to A are the same edges as outgoing edges from RC(A).
    // So NodeMerger only copies the incoming edges to A. So the
    // Edges RC(A)->... are outgoing edges from the reverse strand of the merged
    // node.
    expected.addOutgoingEdge(
        DNAStrand.REVERSE,
        new EdgeTerminal(nodeATA.getNodeId(), DNAStrand.FORWARD));
    expected.addOutgoingEdge(
        DNAStrand.REVERSE,
        new EdgeTerminal(nodeATT.getNodeId(), DNAStrand.FORWARD));

    NodeDiff diff = expected.equalsWithInfo(result.merged_node);
    assertEquals(NodeDiff.NONE, diff);

    HashSet<String> seen_nodeids = new HashSet<String>();
    seen_nodeids.add("ATC");
    seen_nodeids.add("TCG");
    assertEquals(
        seen_nodeids, result.merged_nodeids);

    for (String mergedId : result.merged_nodeids) {
      nodes.remove(mergedId);
    }
    nodes.put(result.merged_node.getNodeId(), result.merged_node);
    List<GraphError> errors = GraphUtil.validateGraph(nodes, K);
    assertTrue(errors.isEmpty());
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
  public void testBreakCycleFlippedStrand() {
    // Consider the special case where we have a cycle.
    // A->B->C-A.
    // Furthermore, supposes that AB > RC(AB), so that after we merge
    // AB the reverse strand of the merge sequence corresponds to the merged
    // strand. In this case, we want to ensure that the code still properly
    // recognizes that C->A forms a cycle.
    final int K = 3;
    // A = ATG
    // B = TGTT
    // C = TTAT
    // SO AB = ATGTT > RC(AB) = AACAT
    String true_merged = "ATGTTAT";

    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addNode("start", "ATG");
    graph.addNode("middle", "TGTT");
    graph.addNode("end", "TTAT");
    GraphNode start = graph.getNode("start");
    GraphNode middle = graph.getNode("middle");
    GraphNode end = graph.getNode("end");
    GraphUtil.addBidirectionalEdge(
        start, DNAStrand.FORWARD, middle, DNAStrand.REVERSE);
    GraphUtil.addBidirectionalEdge(
        middle, DNAStrand.REVERSE, end, DNAStrand.REVERSE);
    GraphUtil.addBidirectionalEdge(
        end, DNAStrand.REVERSE, start, DNAStrand.FORWARD);

    // Validate the graph.
    {
      if (graph.getAllNodes().size() != 3) {
        fail("Test isn't properly setup.");
      }
      // Ensure there is a cycle.
      Set<EdgeTerminal> endTerminals =
          end.getEdgeTerminalsSet(DNAStrand.REVERSE, EdgeDirection.OUTGOING);
      EdgeTerminal startTerminal = new EdgeTerminal(
          start.getNodeId(), DNAStrand.FORWARD);
      if (!endTerminals.contains(startTerminal)) {
        fail("Test isn't setup properly there isn't a cycle.");
      }
    }
    NodesToMerge nodes_to_merge = QuickMergeUtil.findNodesToMerge(
        graph.getAllNodes(), start);

    Sequence true_canonical;
    {
      Sequence true_sequence = new Sequence(
          true_merged, DNAAlphabetFactory.create());
      //DNAStrand true_strand = DNAUtil.canonicaldir(true_sequence);
      true_canonical = DNAUtil.canonicalseq(true_sequence);
    }

    EdgeTerminal expectedStart = new EdgeTerminal(
        start.getNodeId(), DNAStrand.FORWARD);
    EdgeTerminal expectedEnd = new EdgeTerminal(
        end.getNodeId(), DNAStrand.REVERSE);

    assertEquals(nodes_to_merge.start_terminal, expectedStart);
    assertEquals(nodes_to_merge.end_terminal, expectedEnd);
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
  public void testCycleWithIncomingEdge() {
    // This test covers a subgraph that we saw in the staph dataset. The
    // graph is  B->X->RC(Y)->X .
    // In this case the cycle is automatically broken at X because of
    // the incoming edge from B. This means we will try to merge X & Y
    // So the resulting graph should be B->{XY}.
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

    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    nodes.put(branchNode.getNodeId(), branchNode);
    nodes.put(cycleStart.getNodeId(), cycleStart);
    nodes.put(cycleEnd.getNodeId(), cycleEnd);

    NodesToMerge nodesToMerge =
        QuickMergeUtil.findNodesToMerge(nodes, cycleStart);

    int K = 3;
    ChainMergeResult result =
        QuickMergeUtil.mergeLinearChain(nodes, nodesToMerge, K - 1);

    GraphNode expectedMerged = new GraphNode();
    expectedMerged.setNodeId(cycleStart.getNodeId());
    expectedMerged.setSequence(
        new Sequence("AGGAG", DNAAlphabetFactory.create()));

    GraphNode expectedBranch = branchNode.clone();
    GraphUtil.addBidirectionalEdge(
        expectedBranch, DNAStrand.FORWARD, expectedMerged, DNAStrand.REVERSE);

    GraphUtil.addBidirectionalEdge(
        expectedMerged, DNAStrand.FORWARD, expectedMerged, DNAStrand.FORWARD);

    assertEquals(expectedMerged, result.merged_node);
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

    // Since X' = RC(X), there should be a single outgoing edge.
    // The way NodeMerger works this will be on the R strand of the merged
    // node.
    expectedSelf.addOutgoingEdge(
        DNAStrand.REVERSE, new EdgeTerminal("A", DNAStrand.REVERSE));

    GraphNode expectedBorder = new GraphNode();
    expectedBorder.setNodeId("A");
    expectedBorder.setSequence(
        new Sequence("AACTA", DNAAlphabetFactory.create()));

    GraphUtil.addBidirectionalEdge(
        expectedBorder, DNAStrand.FORWARD, expectedSelf, DNAStrand.FORWARD);


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
    NodeDiff diff = expectedSelf.equalsWithInfo(result.merged_node);
    assertEquals(NodeDiff.NONE, diff);
    assertEquals(expectedBorder.getData(), border.getData());
    List<GraphError> graphErrors = GraphUtil.validateGraph(mergedGraph, K);
    assertEquals(0, graphErrors.size());
  }
}
