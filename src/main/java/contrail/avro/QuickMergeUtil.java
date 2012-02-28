package contrail.avro;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import contrail.ContrailConfig;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.LinearChainWalker;
import contrail.graph.NodeMerger;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

/**
 * Some utilities for the QuickMergeStage.
 * @author jlewi
 *
 */
public class QuickMergeUtil {

  /**
   * Data structure which provides information about the nodes which
   * can be merged.
   */
  public static class NodesToMerge {
    public EdgeTerminal start_terminal;
    
    // Last terminal. We may not be able to include this terminal
    // depending on the value of include_final_terminal.
    public EdgeTerminal end_terminal;
    
    // Direction for the merge.
    public EdgeDirection direction;
    /**
     * Whether the final terminal can be included in the merge.
     */
    public boolean include_final_terminal;
  }
  
  /* Return true if this node can be merged.
   * 
   * Suppose we have the chain x-> node.
   * We can only merge node into x if all nodes with 
   * edges  y->node are in memory so that we can update the outgoing edges
   * from y with the new id. 
   * @param nodes: Hashtable of the nodes in memory.
   * @param terminal: The terminal for the edge x->node
   */
  protected static boolean nodeIsMergeable(
      Map<String, GraphNode> nodes, EdgeTerminal terminal) {
  
    // We want to find incoming edges
    // y->RC(strand)
    DNAStrand incoming_strand = DNAStrandUtil.flip(terminal.strand);
    GraphNode node = nodes.get(terminal.nodeId);
    List<EdgeTerminal> incoming_terminals = 
      node.getEdgeTerminals(incoming_strand, EdgeDirection.INCOMING);
    
    for (EdgeTerminal incoming_terminal: incoming_terminals) {
      if (!nodes.containsKey(incoming_terminal.nodeId)) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Find a chain of nodes which can be merged
   * @param nodes_in_memory
   * @param start_node
   * @return: Instance of NodesToMerge or null if no nodes can be merged.
   */
  public static NodesToMerge findNodesToMerge(
      Map<String, GraphNode> nodes_in_memory, GraphNode start_node) {
    NodesToMerge nodes_to_merge = null;

    // Different cases we need to consider
    // h1 -> h2 -> h3 -> node -> t1->t2->t3
    // h1 -> h2 -> h3 -> node
    // node -> t1->t2->t3

    GraphNode head_node;
    DNAStrand head_strand;
    {
      // Starting at node follow the incoming edges for the forward strand.
      TailData head = TailData.findTail(
          nodes_in_memory, start_node, DNAStrand.FORWARD, 
          EdgeDirection.INCOMING);
      
      if (head != null) {
        head_node = nodes_in_memory.get(head.terminal.nodeId);
        head_strand = head.terminal.strand;
      } else {
        // There's no head so set the tail to start at the current node.
        // We might still have a tail  node -> c1, c2, ... that we could
        // merge.
        head_node = start_node;
        head_strand = DNAStrand.FORWARD;
      }
    }

    // Now follow the chain along the forward direction starting at node.
    // The end result is the chain
    //{r1,r2} >> rtail -> c1 -> c2 -> c3 -> node -> c4 -> c5 -> c6 -> ftail >> {f1,f2}
    // We catch cycles by looking for the ftail from rtail, not node.
    // A cycle occurs when we have a repeat along the path e.g
    // rtail --> R --> node -->R-->ftail which can occur in special 
    // circumstances. (What are the circumstances? rtail = ftail? repeated KMers?)  
    // So we need to check no cycles are encountered when merging from 
    // rtail to ftail. We need to do this when considering the path
    // from rtail to ftail because the paths rtail->node and node->ftail
    // may not contain the repeat.

    TailData tail = TailData.findTail(
        nodes_in_memory, head_node, head_strand, 
        EdgeDirection.OUTGOING);

    if (tail == null) {
      // No tail so there are no nodes to merge.
      return null;
    }

    {
      // Check for a cycle by finding the outgoing tail from start_node and
      // checking its the same as the tail from node.
      TailData node_tail = TailData.findTail(
          nodes_in_memory, start_node, DNAStrand.FORWARD, 
          EdgeDirection.OUTGOING);

      boolean has_cycle;
      if (node_tail == null) {
        // Since node_tail is null, we should have the case
        // h1->h2->h3->start_node.
        has_cycle = false;
        if (!tail.terminal.nodeId.equals(start_node.getNodeId())) {
          // The tail doesn't end at start_node. Should this ever happen?
          throw new RuntimeException(
              "Unexpected case happened. We seem to have the tail " +
              "h3->h2->h1->start_node but the tail doesn't end on start_node");
        }
      } else {
        if (!tail.terminal.equals(node_tail.terminal)) {
          throw new NotImplementedException("Need to handle cycles");
        } else {
          has_cycle = false;
        }
      }
    }

    nodes_to_merge = new NodesToMerge();
    nodes_to_merge.start_terminal = new EdgeTerminal(
        head_node.getNodeId(), head_strand);
    nodes_to_merge.end_terminal = tail.terminal;
    nodes_to_merge.direction = EdgeDirection.OUTGOING;
    nodes_to_merge.include_final_terminal = nodeIsMergeable(
        nodes_in_memory, nodes_to_merge.end_terminal);
    return nodes_to_merge;
  }
  
  /**
   * Find all edges which terminate on terminal. And replace them 
   * with edges that terminate on new_terminal.
   * 
   * @param nodes
   * @param terminal
   * @param new_terminal
   */
  protected static void moveIncomingEdges(
      Map<String, GraphNode> nodes, EdgeTerminal terminal, 
      EdgeTerminal new_terminal) {
    
    GraphNode old_node = nodes.get(terminal.nodeId);
    List<EdgeTerminal> source_terminals = old_node.getEdgeTerminals(
        terminal.strand, EdgeDirection.INCOMING);
    
    for (EdgeTerminal source: source_terminals) {
      // We should already have checked that all incoming nodes are in the
      // map.
      GraphNode source_node = nodes.get(source.nodeId);
      source_node.moveOutgoingEdge(
          source.strand, terminal, new_terminal);
    }
  }
  
  /**
   * Data structure for returning the results of merging a chain.
   */
  public static class MergeResult {
    // List of the nodeids that were merged
    public HashSet<String> merged_nodeids;
    
    // The final merged node.
    public GraphNode merged_node;
  }
  
  /**
   * Merge together a set of nodes forming a chain.
   * @param nodes
   * @param nodes_to_merge
   * @param overlap
   * @return
   */
  public static MergeResult mergeLinearChain(
      Map<String, GraphNode> nodes, NodesToMerge nodes_to_merge, int overlap) {
    
    if (nodes_to_merge.direction != EdgeDirection.OUTGOING) {
      throw new NotImplementedException(
          "Currently we only support merging along the outgoing edges");
    }
    
    MergeResult result = new MergeResult();
    result.merged_nodeids = new HashSet<String> ();
    
    EdgeTerminal start_terminal = nodes_to_merge.start_terminal;
    EdgeTerminal end_terminal = nodes_to_merge.end_terminal;
    
    // We start merging from the node associated with head.
    // The merged nodes will have nodeId equal to the node id of where
    // we start. This is necessary, because otherwise an incoming edges
    // to this node will no longer be valid. 
    // Any incoming edges to the reverse complement strand of this node
    // will no longer be valid. However, we know there can be no such
    // edges because An edge  x -> RC(h) implies h -> RC(x)
    // so if RC(x) isn't one of the nodes in memory then h has outdegree
    // 2 so its not part of the chain.
    GraphNode merged_node = nodes.get(start_terminal.nodeId);
    String merged_id = start_terminal.nodeId;
    DNAStrand merged_strand = start_terminal.strand;
    
    LinearChainWalker walker = new LinearChainWalker(
        nodes, start_terminal, EdgeDirection.OUTGOING);
    
    result.merged_nodeids.add(start_terminal.nodeId);
    while (walker.hasNext()) {
      // The last node in the chain requires special treatment
      // because nodes with edges to that node may not be in memory.
      EdgeTerminal merge_terminal = walker.next();
      if (merge_terminal.equals(end_terminal)) {
        if (!nodes_to_merge.include_final_terminal) {
          break;
        }
      }
      
      GraphNode dest = nodes.get(merge_terminal.nodeId);
      result.merged_nodeids.add(merge_terminal.nodeId);
            
      StrandsForEdge strands = 
          StrandsUtil.form(merged_strand, merge_terminal.strand);
      
      // Coverage length is how many KMers overlapping by K-1 bases
      // span the sequence.
      int src_coverage_length = 
          merged_node.getCanonicalSequence().size() - 
          (int) ContrailConfig.K + 1;
      int dest_coverage_length = 
          dest.getCanonicalSequence().size() - 
          (int) ContrailConfig.K + 1;
      
      NodeMerger.MergeResult merge_result = NodeMerger.mergeNodes(
          merged_node, dest, strands, overlap,
          src_coverage_length, dest_coverage_length);
      
      merged_node = merge_result.node;
      merged_node.getData().setNodeId(merged_id);
      
      // Which strand corresponds to the merged strand.
      merged_strand = merge_result.strand; 
    }
    
    merged_node.setNodeId(merged_id);
    
    if (nodes_to_merge.include_final_terminal) {
      // We need to update all the edges which have 
      // outgoing edges to end_terminal.
      EdgeTerminal old_terminal = end_terminal.flip();
      
      // The new terminal will be the reverse complement of the merged_strand.
      EdgeTerminal new_terminal = new EdgeTerminal(
          merged_node.getNodeId(), DNAStrandUtil.flip(merged_strand));
      
      moveIncomingEdges(nodes, old_terminal, new_terminal);      
      result.merged_nodeids.add(end_terminal.nodeId);
    }
                
    result.merged_node = merged_node;
    return result;
  }
}
