package contrail.avro;

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

import contrail.graph.EdgeData;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.NeighborData;
import contrail.graph.SimpleGraphBuilder;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.util.ListUtil;
import contrail.util.Tuple;
// Extend QuickMergeUtil so we can test protected methods
public class TestQuickMergeUtil extends QuickMergeUtil {

  // Random number generator.
  private Random generator;
  @Before 
  public void setUp() {
    // Create a random generator.
    generator = new Random(107);
  }
  
  @Test
  public void testNodeIsMergeable() {
    {
      // Create a node which is mergeable.
      GraphNode node = new GraphNode();
      node.getData().setNodeId("node_to_merge");
      
      EdgeTerminal terminal_to_merge = new EdgeTerminal(
          node.getNodeId(), DNAStrandUtil.random(generator));
      
      // Create an incoming edge
      DNAStrand strand_for_edge = DNAStrandUtil.flip(terminal_to_merge.strand);
      GraphNode other_node = new GraphNode();
      other_node.getData().setNodeId("node_for_edge");
      EdgeTerminal edge_terminal = new EdgeTerminal(
          other_node.getNodeId(), DNAStrandUtil.random(generator));      
      node.addIncomingEdge(strand_for_edge, edge_terminal);
      
      HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
      nodes.put(node.getNodeId(), node);
      
      // Since other_node is not in the map of nodes it should not be 
      // mergeable.
      assertFalse(QuickMergeUtil.nodeIsMergeable(nodes, terminal_to_merge));
                  
      // When other_node is in the map of nodes it should be mergeable.
      nodes.put(other_node.getNodeId(), other_node);
      assertTrue(QuickMergeUtil.nodeIsMergeable(nodes, terminal_to_merge));
    }
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
  private ArrayList<ChainNode> constructChain(int length) {
    ArrayList<ChainNode> chain = new ArrayList<ChainNode>();

    // Construct the nodes.
    for (int pos = 0; pos < length; pos++) {
      // Construct a graph node.
      GraphNode node = new GraphNode();
      GraphNodeData node_data = new GraphNodeData();
      node.setData(node_data);
      node_data.setNodeId("node_" + pos);
      node_data.setNeighbors(new ArrayList<NeighborData>());
      
      ChainNode chain_node = new ChainNode();
      chain_node.graph_node = node;
      chain_node.dna_direction = DNAStrandUtil.random(generator);
      chain.add(chain_node);
    }
    
    // Now connect the nodes.
    // TODO(jlewi): We should add incoming and outgoing edges to the first
    // and last node so that we test that walker stops when the node isn't
    // in memory.
    for (int pos = 0; pos < length; pos++) {
      // Add the outgoing edge.
      if (pos + 1 < length) {
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
   * Construct a linear chain from the given sequence using KMers of length K.
   * @param length
   * @return
   */
  private ArrayList<ChainNode> constructChainForSequence(
      String full_sequence, int K) {
    
    // TODO(jlewi): We can replace this function with 
    // conStructChainForSequence.    
    ArrayList<ChainNode> chain = new ArrayList<ChainNode>();

    // Construct the nodes.
    for (int pos = 0; pos <= full_sequence.length() - K; pos++) {
      // Construct a graph node.
      GraphNode node = new GraphNode();
      node.setNodeId("node_" + pos);
      
      Sequence sequence = new Sequence(
          full_sequence.substring(pos, pos + K), DNAAlphabetFactory.create());
      node.setCanonicalSequence(DNAUtil.canonicalseq(sequence));
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
   * Run a trial to test findNodesTomerge
   * 
   * This function assumes, that chain of nodes forms a linear chain,
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
    
    // We should be able to include the last node because there are
    // no outgoing edges.
    assertTrue(merge_info.include_final_terminal);
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
  
  @Test
  public void testfindNodesToMerge() {
    ArrayList<ChainNode> chain_nodes = constructChain(5);
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
      
      {
        // The nodes for the merge should still be the chain ends, but
        // include_final_terminal should be false.
        QuickMergeUtil.NodesToMerge merge_info = 
            QuickMergeUtil.findNodesToMerge(
                 nodes, chain_nodes.get(start_search).graph_node);
        
        assertEquals(chain_ends.first, merge_info.start_terminal);
        assertEquals(chain_ends.second, merge_info.end_terminal);
        
        // We should be able to include the last node because there are
        // no outgoing edges.
        assertFalse(merge_info.include_final_terminal);
      }
      {
        // Add the node to the list of nodes and check final node is now
        // mergeable.
        nodes.put(tail_node.getNodeId(), tail_node);
        // The nodes for the merge should still be the chain ends, but
        // include_final_terminal should be false.
        QuickMergeUtil.NodesToMerge merge_info = 
            QuickMergeUtil.findNodesToMerge(
                 nodes, chain_nodes.get(start_search).graph_node);
        
        assertEquals(chain_ends.first, merge_info.start_terminal);
        assertEquals(chain_ends.second, merge_info.end_terminal);
        
        // We should be able to include the last node because there are
        // no outgoing edges.
        assertTrue(merge_info.include_final_terminal);
      }
    }
  }
  
  @Test
  public void testMergeLinearChain() {
    for (int trial = 0; trial < 10; trial++) {
      // Even though we have repeats the merge still work, because
      // the way we construct the chain, we have one node for each instance
      // rather than representing all instances of the same KMer using a sngle
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
      QuickMergeUtil.MergeResult result =
          QuickMergeUtil.mergeLinearChain(nodes, merge_info, overlap);
      
      Sequence full_canonical = new Sequence(
          full_sequence, DNAAlphabetFactory.create());
      
      full_canonical = DNAUtil.canonicalseq(full_canonical);
  
      // Check the sequence equals the original sequence.
      assertEquals(full_canonical, result.merged_node.getCanonicalSequence());
      
      Set<String> expected_merged_ids = nodes.keySet();  
      assertEquals(expected_merged_ids, result.merged_nodeids);
    }
  }
  
//  @Test
//  public void testCycle() {
//    // Consider the special case where we have a cycle
//    // e.g: ATTCATT with K = 3 gives rise to
//    // ATT->TTC->TCA->ATT
//    // In this case no nodes should be merged and all should be marked
//    // as being seen.    
//    final int K = 3;
//    SimpleGraphBuilder graph = new SimpleGraphBuilder();
//    graph.addKMersForString("ATTCATT", K);
//    
//    // The KMers where we should start the search.
//    // We start at all KMers in the cycle to make sure te start doesn't matter.
//    String[] start_kmers = {"ATT", "TTC", "TCA", "CAT"};
//    for (String start: start_kmers) {
//      EdgeTerminal terminal = graph.findEdgeTerminalForSequence(start);
//      GraphNode start_node = graph.getNode(terminal.nodeId);
//      NodesToMerge nodes_to_merge = QuickMergeUtil.findNodesToMerge(
//          graph.getAllNodes(), start_node);
//      
//      assertTrue(nodes_to_merge.hit_cycle);
//      assertEquals(null, nodes_to_merge.start_terminal);
//      assertEquals(null, nodes_to_merge.end_terminal);
//      
//      List<String> seen_nodeids = new ArrayList<String>();
//      seen_nodeids.addAll(graph.getAllNodes().keySet());
//      assertEquals(
//          graph.getAllNodes().keySet(), nodes_to_merge.nodeids_visited);     
//    }  
//  }
  
  @Test
  public void testMergeRepeated() {
    // Consider the special case where we have a chain in which 
    // some sequence and its reverse complement appears.
    // Note: if a->y->b ...-c>RC(y)->d
    // Then a->y implies RC(y)->RC(A)
    // which implies either 
    // 1) d = RC(a)
    // 2) d != RC(A) then RC(Y) has two outgoing edges
    // RC(y)->d, and RC(y)->RC(a)  in this case we can't merge RC(y) with d 
    // of each other.
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
    }
    
    Hashtable<String, GraphNode> nodes = graph.getAllNodes();
    
    GraphNode start_node;
    {
      int start = generator.nextInt(true_sequence_str.length() - K + 1);
      EdgeTerminal terminal = graph.findEdgeTerminalForSequence(
          true_sequence_str.substring(start, start + K));
      start_node = graph.getNode(terminal.nodeId);
    }
    NodesToMerge nodes_to_merge = QuickMergeUtil.findNodesToMerge(
        graph.getAllNodes(), start_node);
    
    MergeResult result = QuickMergeUtil.mergeLinearChain(
        nodes, nodes_to_merge, K - 1);
    
    // Check the merged sequence is correct.
    assertEquals(true_canonical, result.merged_node.getCanonicalSequence());
                
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
  //Hashtable<String, GraphNode> nodes = graph.getAllNodes();
  String[] true_merged = {"ATCGCAT", "TCGCATC", "CGCATCG", "GCATCGA"};
  
  for (int start = 0; start <= true_sequence_str.length() - K - 1; ++start) {
    SimpleGraphBuilder graph = new SimpleGraphBuilder(); 
    graph.addKMersForString(true_sequence_str, K);
    // Get the KMer corresponding to the start
    String start_kmer = true_merged[start].substring(0, K);
    
    EdgeTerminal start_terminal = graph.findEdgeTerminalForSequence(start_kmer);    
    GraphNode start_node = graph.getAllNodes().get(start_terminal.nodeId);
    NodesToMerge nodes_to_merge = QuickMergeUtil.findNodesToMerge(
        graph.getAllNodes(), start_node);

    Sequence true_canonical;
    {
      Sequence true_sequence = new Sequence(
          true_merged[start], DNAAlphabetFactory.create());
      //DNAStrand true_strand = DNAUtil.canonicaldir(true_sequence);
      true_canonical = DNAUtil.canonicalseq(true_sequence);
    }
    {
      // Check nodes_to_merge is correct.
      EdgeTerminal expected_start;
      EdgeTerminal expected_end;
      {
        Sequence merged = new Sequence(
            true_merged[start], DNAAlphabetFactory.create());
        merged = DNAUtil.canonicalseq(merged);
        String first_kmer = merged.subSequence(0, K).toString();
        String last_kmer = merged.subSequence(
            merged.size() - K, merged.size()).toString();
        
      expected_start = graph.findEdgeTerminalForSequence(first_kmer);
      expected_end = graph.findEdgeTerminalForSequence(last_kmer);
      }
//      String first_kmer = true_merged[start].substring(0, K);
//      String last_kmer = true_merged[start].substring(
//          true_merged[start].length() - K, true_merged[start].length());
//      
//      EdgeTerminal expected_start = 
//          graph.findEdgeTerminalForSequence(first_kmer);
//      EdgeTerminal expected_end = 
//          graph.findEdgeTerminalForSequence(last_kmer);
//      
//      // The code always starts on the forward strand.
//      if (expected_start.strand == DNAStrand.REVERSE) {
//        expected_start = expected_start.flip();
//        expected_end = expected_end.flip();
//      }
      assertEquals(nodes_to_merge.start_terminal, expected_start);
      assertEquals(nodes_to_merge.end_terminal, expected_end);
      assertTrue(nodes_to_merge.include_final_terminal);
      assertEquals(
          graph.getAllNodes().keySet(), nodes_to_merge.nodeids_visited);
    }
    MergeResult result = QuickMergeUtil.mergeLinearChain(
        graph.getAllNodes(), nodes_to_merge, K - 1);
    
    // Check the merged sequence is correct.
    assertEquals(true_canonical, result.merged_node.getCanonicalSequence());
    
    // Check the cycle is closed. There should be an edge to the start kmer
    Sequence last_kmer = true_canonical.subSequence(0, K);
    
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
//      for (int trial = 0; trial < 10; trial++) {
//        // Even though we have repeats the merge still work, because
//        // the way we construct the chain, we have one node for each instance
//        // rather than representing all instances of the same KMer using a sngle
//        // node.
//        int K = generator.nextInt(20) + 3;
//        int length = generator.nextInt(100) + K;
//        String full_sequence = AlphabetUtil.randomString(
//            generator, length, DNAAlphabetFactory.create());
//        
//        int overlap = K - 1;
//        ArrayList<ChainNode> chain_nodes =
//            constructChainForSequence(full_sequence, K);
//        
//        // Add a cycle by connecting the last node in the chain with the first
//        {
//          GraphNode node = chain_nodes.get(chain_nodes.size() - 1).graph_node;
//          DNAStrand strand= chain_nodes.get(
//              chain_nodes.size() -1).dna_direction;
//          
//          EdgeTerminal head = new EdgeTerminal(
//              chain_nodes.get(0).graph_node.getNodeId(), 
//              chain_nodes.get(0).dna_direction);
//          
//          node.addOutgoingEdge(strand, head);
//        }
//        {
//          GraphNode node = chain_nodes.get(0).graph_node;
//          DNAStrand strand= chain_nodes.get(0).dna_direction;
//          EdgeTerminal tail = new EdgeTerminal(
//              chain_nodes.get(chain_nodes.size() - 1).graph_node.getNodeId(), 
//              chain_nodes.get(chain_nodes.size() - 1).dna_direction);
//          
//          node.addIncomingEdge(strand, tail);
//        }
//        
//        HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
//        for (ChainNode link: chain_nodes) {
//          nodes.put(link.graph_node.getNodeId(), link.graph_node);
//        }
//        
//        int chain_start = generator.nextInt(chain_nodes.size());
//        EdgeTerminal start_walk = new EdgeTerminal(
//            chain_nodes.get(chain_start).graph_node.getNodeId(), 
//            chain_nodes.get(chain_start).dna_direction);
//        
//        Tuple<EdgeTerminal, EdgeTerminal> terminals = 
//            QuickMergeUtil.breakCycle(nodes, start_walk);
//        
//        EdgeTerminal expected_start = new EdgeTerminal(
//            chain_nodes.get(1).graph_node.getNodeId(), 
//            chain_nodes.get(1).dna_direction);
//        EdgeTerminal expected_end = new EdgeTerminal(
//            chain_nodes.get(chain_nodes.size() - 2).graph_node.getNodeId(), 
//            chain_nodes.get(chain_nodes.size() - 2).dna_direction);
//        assertEquals(expected_start, terminals.first);
//        assertEquals(expected_end, terminals.second);
//      }
  }
}
