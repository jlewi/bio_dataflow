package contrail.avro;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.graph.EdgeData;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.NeighborData;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
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
    
    ArrayList<ChainNode> chain = new ArrayList<ChainNode>();

    // Construct the nodes.
    for (int pos = 0; pos <= full_sequence.length() - K; pos++) {
      // Construct a graph node.
      GraphNode node = new GraphNode();
      GraphNodeData node_data = new GraphNodeData();
      node.setData(node_data);
      node_data.setNodeId("node_" + pos);
      
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
    int K = 3;
    String full_sequence = AlphabetUtil.randomString(
        generator, 6, DNAAlphabetFactory.create());
    
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
        QuickMergeUtil.mergeLinearChain(nodes, merge_info);
    
    Sequence full_canonical = new Sequence(
        full_sequence, DNAAlphabetFactory.create());
    
    full_canonical = DNAUtil.canonicalseq(full_canonical);

    // Check the sequence equals the original sequence.
    assertEquals(full_canonical, result);
    
    // Check the list of merged_nodeids.
    List<String> expected_merged_ids = new ArrayList<String>();
    
    //assertEquals(result.merged_nodeids.si)
  }
}
