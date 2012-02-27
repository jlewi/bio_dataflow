package contrail.avro;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;

import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;;
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

}
