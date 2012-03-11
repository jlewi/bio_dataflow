// Author: Jeremy Lewi(jeremy@lewi.us)
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
    public Boolean include_final_terminal;
    
    // Whether the first terminal can be included in the merge.
    public Boolean include_first_terminal;
    
    // If start_terminal and end_terminal are non null
    // this be a list of nodes [start, end] (inclusive).
    // If start_terminal and end_terminal are null then this is a list of
    // nodes which can't be merged.
    public HashSet<String> nodeids_visited;
    
    // Whether we hit a cycle or not
    public Boolean hit_cycle;
  }
  
  /* Return true if this node can be merged.
   * 
   * Suppose we have the chain x-> z.
   * We can only merge z into x if all nodes with 
   * edges  y->RC(z) are in memory so that we can update the outgoing edges
   * from y with the new id. 
   * @param nodes: Hashtable of the nodes in memory.
   * @param terminal: The terminal for the edge x->node
   */
//  protected static boolean nodeIsMergeable(
//      Map<String, GraphNode> nodes, EdgeTerminal terminal) {
//  
//    // We want to find incoming edges
//    // y->RC(strand)
//    DNAStrand incoming_strand = DNAStrandUtil.flip(terminal.strand);
//    GraphNode node = nodes.get(terminal.nodeId);
//    List<EdgeTerminal> incoming_terminals = 
//      node.getEdgeTerminals(incoming_strand, EdgeDirection.INCOMING);
//    
//    for (EdgeTerminal incoming_terminal: incoming_terminals) {
//      if (!nodes.containsKey(incoming_terminal.nodeId)) {
//        return false;
//      }
//    }
//    return true;
//  }
    
  /**
   * Returns true if the incoming edges to terminal are in memory.
   * 
   * @param nodes: Hashtable of the nodes in memory.
   * @param terminal: The terminal for the edge x->node.
   * @return
   */
  protected static boolean checkIncomingEdgesAreInMemory(
      Map<String, GraphNode> nodes, EdgeTerminal terminal) {
    // We want to find incoming edges.
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
    // Different cases we need to consider
    // h1 -> h2 -> h3 -> node -> t1->t2->t3
    // h1 -> h2 -> h3 -> node
    // node -> t1->t2->t3   
    EdgeTerminal head_terminal;
    
    NodesToMerge result = new NodesToMerge();
    result.direction = EdgeDirection.OUTGOING;
    result.hit_cycle = false;
    result.nodeids_visited = new HashSet<String> ();
    result.nodeids_visited.add(start_node.getNodeId());
    result.include_first_terminal = false;
    result.include_final_terminal = false;
    
    {
      // Starting at node follow the incoming edges for the forward strand.
      TailData head = TailData.findTail(
          nodes_in_memory, start_node, DNAStrand.FORWARD, 
          EdgeDirection.INCOMING);
        
      if (head != null) {
        head_terminal = head.terminal;

        if (head.hit_cycle) {
          // In the case of a perfect cycle (i.e. no branch points) we 
          // can compress the sequence by picking any arbitrary point
          // in the cycle as the start of the merge. 
          result.nodeids_visited = head.nodes_in_tail;
          result.hit_cycle = true;
          
          // We will break the cycle at start node.
          // So we merge from the start node to the incoming terminal to
          // the start terminal.
          result.start_terminal = new EdgeTerminal(
              start_node.getNodeId(), DNAStrand.FORWARD);
          List<EdgeTerminal> incoming_terminals = start_node.getEdgeTerminals(
              DNAStrand.FORWARD, EdgeDirection.INCOMING);
          if (incoming_terminals.size() != 1) {
            throw new RuntimeException(
                "Something went wrong with cycle detection; there should be " +
                "a single incoming edge");
          }
          result.end_terminal = incoming_terminals.get(0);
          // Since its a cycle, we can include both the start
          // and end terminals. The cycle gets encoded just by adding edges
          // to itself to the node.
          result.include_first_terminal = true;
          result.include_final_terminal = true;
          return result;
        }
      } else {
        // There's no head so set the tail to start at the current node.
        // We might still have a tail  node -> c1, c2, ... that we could
        // merge.
        head_terminal = new EdgeTerminal(
            start_node.getNodeId(), DNAStrand.FORWARD);
      }
    }
      
    start_node = nodes_in_memory.get(head_terminal.nodeId);
    
    TailData full_tail = TailData.findTail(
        nodes_in_memory, start_node,  head_terminal.strand, 
        EdgeDirection.OUTGOING);
    
    if (full_tail != null) {
      result.start_terminal = head_terminal;
      result.end_terminal = full_tail.terminal;
      result.nodeids_visited.addAll(full_tail.nodes_in_tail);
      
      // We can only include the first terminal if all its incoming
      // edges are in memory.
      result.include_first_terminal = checkIncomingEdgesAreInMemory(
          nodes_in_memory, result.start_terminal);
      result.include_final_terminal = checkIncomingEdgesAreInMemory(
          nodes_in_memory, result.end_terminal.flip());
    }
    
    
    return result;
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
  public static class ChainMergeResult {
    // List of the nodeids that were merged
    public HashSet<String> merged_nodeids;
    
    // The final merged node.
    public GraphNode merged_node;   
  }
  
  /**
   * An internal function for doing the merge in the special case
   * when the chain has no internal nodes; i.e we are merging
   * x1->x2.
   * @param nodes
   * @param nodes_to_merge
   * @param overlap
   * @return
   */
  protected static ChainMergeResult mergeChainNoInterior(
      Map<String, GraphNode> nodes, NodesToMerge nodes_to_merge, int overlap) {
    // This function should only be called from mergeLinearChain.
    // Furthermore, we assume this function is only invoked 
    // if its already been checked that we have no interior nodes.
    ChainMergeResult result = new ChainMergeResult();
    result.merged_nodeids = new HashSet<String>();
    
    if (nodes_to_merge.hit_cycle) {
      GraphNode src_node = nodes.get(nodes_to_merge.start_terminal.nodeId);
      GraphNode dest_node = nodes.get(nodes_to_merge.end_terminal.nodeId);
      
      StrandsForEdge strands = StrandsUtil.form(
          nodes_to_merge.start_terminal.strand, 
          nodes_to_merge.end_terminal.strand);
      
      NodeMerger.MergeResult merge_result = NodeMerger.mergeNodes(
          src_node, dest_node, strands, overlap);
      
      GraphNode merged_node = merge_result.node;
      
      // We are merging a perfect cycle. 
      // e.g ATC->TCG->CGC->GCA->CAT
      // So the merged sequence is ATCGCAT
      // and has incoming/outgoing edges to itself. So we need to move
      // the incoming edge to ATC. E.g CAT->ATC needs to become
      // ATCGCAT -> ATCGCAT
      // CAT->ATC implies RC(ATC)->RC(CAT)
      // So we need to move the    
      EdgeTerminal end_terminal = nodes_to_merge.end_terminal;
      DNAStrand strand = DNAStrandUtil.flip(merge_result.strand);
      EdgeTerminal old_terminal = end_terminal.flip();        
      EdgeTerminal new_terminal = new EdgeTerminal(
          merged_node.getNodeId(), strand);
      //moveOutgoingEdges(nodes, old_terminal, new_terminal);    
      //GraphNode source_node = nodes.get(source.nodeId);
      merged_node.moveOutgoingEdge(
          strand, old_terminal, new_terminal);
      
      result.merged_node = merged_node;
      result.merged_nodeids.add(src_node.getNodeId());
      result.merged_nodeids.add(dest_node.getNodeId());
      return result;
    }
    

    return result;
  }
  
  /**
   * Merge together a set of nodes forming a chain.
   * @param nodes
   * @param nodes_to_merge
   * @param overlap
   * @return
   */
  public static ChainMergeResult mergeLinearChain(
      Map<String, GraphNode> nodes, NodesToMerge nodes_to_merge, int overlap) {
    
    if (nodes_to_merge.direction != EdgeDirection.OUTGOING) {
      throw new NotImplementedException(
          "Currently we only support merging along the outgoing edges");
    }
    
    ChainMergeResult result;


    //*********************************************************************
    // ToDO(jlewi): Comment below is inaccurate clean it up.
    //*********************************************************************
    
//    EdgeTerminal start_terminal = nodes_to_merge.start_terminal;
//    EdgeTerminal end_terminal = nodes_to_merge.end_terminal;

    // Suppose we have the chain 
    // x1->x2,...,xn-1->xn
    // The ends of the chain require special care because we potentially
    // need to move edges
    // y_i->x1  and  z_i->RC(xn)
    // So The merge proceeds as follows
    // Merge the interior nodes x1->Merged(x2,...,xn-1)->xn
    //
    // We can include x1 in the merge if 1) all y_i are in memory
    // or 2) the merged sequence corresponds to the same strand
    // as x1 and we assign the merged strand an id id(x1). IN this 
    // case edges y_i->x1 don't need to change.
    // The same holds for whether or not we can include xn.
    
    // Check if either the first or end terminals has all edges in
    // memory and if so then they can be merged.
        
    // We start merging at start_terminal. However, we can only
    // include start_terminal if all its incoming edges are in memory
    // so they can be moved to the new merged node. 
    // Note, we could potentially include the first terminal even if its
    // incoming edges aren't in memory provided the node id doesn't change.
    // This requires that 1) we assign the merged node the id start_terminal.id
    // and 2) that the strand for the start terminal corresponds to the same
    // strand in the merged node. We do the former but don't check for the 
    // latter, so we only include the first node if all its edges are in memory.
    // The merged nodes will have nodeId equal to the node id of where
    // we start. This is necessary, because otherwise an incoming edges
    // to this node will no longer be valid. 
    // Any incoming edges to the reverse complement strand of this node
    // will no longer be valid. However, we know there can be no such
    // edges because An edge  x -> RC(h) implies h -> RC(x)
    // so if RC(x) isn't one of the nodes in memory then h has outdegree
    // 2 so its not part of the chain.    
    GraphNode merged_node;
    String merged_id;
    DNAStrand merged_strand;
    
    {
      // Merge the interior nodes.
      GraphNode start_node = 
          nodes.get(nodes_to_merge.start_terminal.nodeId);
      TailData start_tail = start_node.getTail(
          nodes_to_merge.start_terminal.strand, EdgeDirection.OUTGOING);
      
      if (start_tail.terminal.equals(nodes_to_merge.end_terminal)) {
        // There aren't any interior nodes.
        return mergeChainNoInterior(nodes, nodes_to_merge, overlap);
      }
      
      // Initialize here because we don't want to initialize if 
      // we have the special case mergeChainNoInterior.
      result = new ChainMergeResult();
      result.merged_nodeids = new HashSet<String>();
      
      GraphNode end_node = 
          nodes.get(nodes_to_merge.end_terminal.nodeId);
      TailData end_tail = end_node.getTail(
          nodes_to_merge.end_terminal.strand, EdgeDirection.INCOMING);
      
      EdgeTerminal internal_start = start_tail.terminal;
      EdgeTerminal internal_end = end_tail.terminal;
      // Merge the internal nodes.
      merged_node = nodes.get(internal_start.nodeId);
      merged_id = internal_start.nodeId;
      merged_strand = internal_start.strand;
      
      LinearChainWalker walker = new LinearChainWalker(
          nodes, internal_start, EdgeDirection.OUTGOING);
      
      result.merged_nodeids.add(internal_start.nodeId);
      while (walker.hasNext()) {
        // The last node in the chain requires special treatment
        // because nodes with edges to that node may not be in memory.
        EdgeTerminal merge_terminal = walker.next();
        
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
        
        if (merge_terminal.equals(internal_end)) {
          // This is the last internal node so break out.
          break;
        }
      }
    }
    
    boolean merged_first = false;
    boolean merged_last = false;
    
    // Whether we need to move the edges to the start and
    // end nodes. We can't move the edges until we finalize merged
    // id.
    boolean move_edges_to_start = false;
    boolean move_edges_to_rc_end = false;
    
    // Check if either the first or end terminals has all edges in
    // memory and if so then they can be merged.
    if (checkIncomingEdgesAreInMemory(nodes, nodes_to_merge.start_terminal)) {
      // All edges are in memory so do the move.
      merged_first = true;
      
      EdgeTerminal merge_terminal = nodes_to_merge.start_terminal;
      GraphNode first_node = nodes.get(merge_terminal.nodeId);      
      result.merged_nodeids.add(first_node.getNodeId());
            
      StrandsForEdge strands = 
          StrandsUtil.form(merge_terminal.strand, merged_strand);
      
      // Coverage length is how many KMers overlapping by K-1 bases
      // span the sequence.
      int src_coverage_length = 
          first_node.getCanonicalSequence().size() - 
          (int) ContrailConfig.K + 1;
      int dest_coverage_length = 
          merged_node.getCanonicalSequence().size() - 
          (int) ContrailConfig.K + 1;
      
      NodeMerger.MergeResult merge_result = NodeMerger.mergeNodes(
          first_node, merged_node, strands, overlap,
          src_coverage_length, dest_coverage_length);
      
      merged_node = merge_result.node;
      
      // Which strand corresponds to the merged strand.
      merged_strand = merge_result.strand;
      merged_first = true;
      
      merged_id = merge_terminal.nodeId;
      // Mark the edges for moving.
      move_edges_to_start = true;
    }

    if (checkIncomingEdgesAreInMemory(
        nodes, nodes_to_merge.end_terminal.flip())) {
      // All edges are in memory so do the move.
      merged_last = true;
      
      EdgeTerminal merge_terminal = nodes_to_merge.end_terminal;
      GraphNode last_node = nodes.get(merge_terminal.nodeId);      
      result.merged_nodeids.add(last_node.getNodeId());
            
      StrandsForEdge strands = 
          StrandsUtil.form(merged_strand, merge_terminal.strand);
      
      // Coverage length is how many KMers overlapping by K-1 bases
      // span the sequence.
      int src_coverage_length = 
          merged_node.getCanonicalSequence().size() - 
          (int) ContrailConfig.K + 1;
      int dest_coverage_length = 
          last_node.getCanonicalSequence().size() - 
          (int) ContrailConfig.K + 1;
      
      NodeMerger.MergeResult merge_result = NodeMerger.mergeNodes(
          merged_node, last_node, strands, overlap,
          src_coverage_length, dest_coverage_length);
      
      merged_node = merge_result.node;
      
      // Which strand corresponds to the merged strand.
      merged_strand = merge_result.strand;
      merged_last = true;
      
      // Mark the edges for moving.
      move_edges_to_rc_end = true;  
    }
    
    // If we can't include the first terminal then we need to advance
    // the chain before starting the merge.
//    if (nodes_to_merge.include_first_terminal) {
//      merged_node = nodes.get(start_terminal.nodeId);
//      merged_id = start_terminal.nodeId;
//      merged_strand = start_terminal.strand;
//    } else {
//      GraphNode node = nodes.get(start_terminal.nodeId);
//      TailData tail = 
//          node.getTail(start_terminal.strand, EdgeDirection.OUTGOING);
//      
//      merged_node = nodes.get(tail.terminal.nodeId);
//      merged_id = merged_node.getNodeId();
//      merged_strand = tail.terminal.strand;  
//    }
//    
 
    // The node id should no longer change because we are about
    // to move other edges.
    merged_node.setNodeId(merged_id);
    
    if (nodes_to_merge.hit_cycle) {
      // Sanity check, we should have included the first and last nodes.
      if (!merged_first || !merged_last) {
        throw new RuntimeException(
            "We have a perfect cycle but the first and last nodes weren't " +
            "merged");
      }
      // We are merging a perfect cycle. 
      // e.g ATC->TCG->CGC->GCA->CAT
      // So the merged sequence is ATCGCAT
      // and has incoming/outgoing edges to itself. So we need to move
      // the incoming edge to ATC. E.g CAT->ATC needs to become
      // ATCGCAT -> ATCGCAT
      // CAT->ATC implies RC(ATC)->RC(CAT)
      // So we need to move the 
    
      EdgeTerminal end_terminal = nodes_to_merge.end_terminal;
      DNAStrand strand = DNAStrandUtil.flip(merged_strand);
      EdgeTerminal old_terminal = end_terminal.flip();        
      EdgeTerminal new_terminal = new EdgeTerminal(
          merged_node.getNodeId(), strand);
      //moveOutgoingEdges(nodes, old_terminal, new_terminal);    
      //GraphNode source_node = nodes.get(source.nodeId);
      merged_node.moveOutgoingEdge(
          strand, old_terminal, new_terminal);
      
      result.merged_node = merged_node;
      return result;
    }
    
    if (move_edges_to_start) {
      // We need to move incoming edges to the start terminal.
      EdgeTerminal old_terminal = nodes_to_merge.start_terminal;
      
      EdgeTerminal new_terminal = new EdgeTerminal(
          merged_node.getNodeId(), merged_strand);
      
      if (!old_terminal.equals(new_terminal)) {
        moveIncomingEdges(nodes, old_terminal, new_terminal);
      }
    }
                
    if (move_edges_to_rc_end) {      
      // We need to move edges 
      // z->RC(xn)
      EdgeTerminal old_terminal = nodes_to_merge.end_terminal.flip();
      EdgeTerminal new_terminal = new EdgeTerminal(
          merged_node.getNodeId(), DNAStrandUtil.flip(merged_strand));
      
      // The nodeid for the merged node should be the same as the start
      // terminal, but the strand might have changed. In which case we
      // need to move the incoming edges
      if (!old_terminal.equals(new_terminal)) {
        moveIncomingEdges(nodes, old_terminal, new_terminal);
      }   
    }
    result.merged_node = merged_node;
    return result;
  }
}
