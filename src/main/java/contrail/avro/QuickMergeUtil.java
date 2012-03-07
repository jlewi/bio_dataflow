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
import contrail.util.Tuple;

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
    public Boolean include_final_terminal;
    
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
  
  
  // TODO(jlewi): Figure out what to do with this code.
//  protected static Tuple<EdgeTerminal, EdgeTerminal> breakCycle(
//      Map<String, GraphNode> nodes, EdgeTerminal head) {
//    // This function should only be called after we have detected a cycle.
//    // We have a cycle.
//    // Try to break the cycle. 
//    // Case 1: suppose we have the read ATTATT
//    // which produces the cycle ATT->TTA->TAT->ATT->
//    // in this case we get a cycle because we have a repeated KMer
//    // at the start and of the read. We can use the read alignment
//    // tags to identify the start and end of the read and therefore
//    // where to break the chain.
//    LinearChainWalker walker = new LinearChainWalker(
//        nodes, head, EdgeDirection.OUTGOING);
//    
//    // List of edges where we could break the chain.
//    HashSet<EdgeTerminal> breakpoints = new HashSet<EdgeTerminal>();
//    
//    // Check if we can break the cycle at the head
//    if (nodeIsAtEndsOfRead(nodes.get(head.nodeId))) {
//      breakpoints.add(head);
//    }
//    
//  }
  /**
   * This function returns the start and end terminal for the chain of nodes
   * to merge. This function takes care of detecting cycles and breaking
   * them if possible.
   * 
   * @param nodes
   * @param head
   * @return
   */
//  protected static NodesToMerge findCycle(
//      Map<String, GraphNode> nodes, EdgeTerminal head) {
//    // We could have a sequence  >R->r2->N->f1-> R
//    // e.g suppose K = 3 and we have the read
//    // ATT->TTA-->TAT->ATT then this will produce a cycle .
//    // To avoid cycling around forever, the findTail methods stops
//    // as soon as it hits a node we've already seen.
//    // We thus detect cycles as follows.
//    // 1. Follow the incoming edge as far as we can go (this is head).
//    // 2. Follow the outgoing edges as far as we can go; recording the 
//    // nodes visited. when the chain ends, check if the last node
//    // has a tail and if it does, check if the node is one we've already
//    // seen.
//    // 3. If we have a cycle we can merge all nodes in between the repeated
//    // nodes.
//    NodesToMerge result = new NodesToMerge();
//    result.nodeids_visited = new HashSet<String>();
//    LinearChainWalker walker = new LinearChainWalker(
//        nodes, head, EdgeDirection.OUTGOING);
//    
//    result.nodeids_visited.add(head.nodeId);
//    //seen_nodes.add(head.nodeId);
//    EdgeTerminal last = null;
//    while (walker.hasNext()) {
//       last = walker.next();
//       result.nodeids_visited.add(last.nodeId);
//    }
//    
//    if (last == null) {
//      // Then there isn't any tail so return null
//      return result;
//    }
//    
//    if (walker.hitCycle()) {
//      // So we started at head->n1 ->n2->n3,->n4->head
//      // To check this we check that the next node in the chain is
//      // where we started from
//      GraphNode node = nodes.get(last.nodeId);
//      TailData last_tail = node.getTail(last.strand, EdgeDirection.OUTGOING);
//      if (last_tail != null && last_tail.terminal.equals(head)) {
//        // We have a cycle, for now just treat this as the nodes not
//        // being able to 
//        // Try to break the cycle. 
//        // Case 1: suppose we have the read ATTATT
//        // which produces the cycle ATT->TTA->TAT->ATT->
//        // in this case we get a cycle because we have a repeated KMer
//        // at the start and of the read. We can use the read alignment
//        // tags to identify the start and end of the read and therefore
//        // where to break the chain.
//        result.hit_cycle = true;
//        return result;
//      } else {
//        throw new RuntimeException(
//            "Looks like we have a repeated node that isn't a cycle. "+
//            "What to do?");
//      }
//      
//      
////      TailData head_tail = TailData.findTail(
////          nodes, nodes.get(last_tail.terminal.nodeId), 
////          last_tail.terminal.strand, EdgeDirection.INCOMING);
////      
////      head = head_tail.terminal;
//    }
//    
//    result.start_terminal = head;
//    result.end_terminal = last;
//    result.direction = EdgeDirection.OUTGOING;
//    return result;
//  }
  
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
          // Since its a cycle, we don't include the final terminal
          // because we need to break the cycle.
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
      result.include_final_terminal = nodeIsMergeable(
          nodes_in_memory, result.end_terminal);
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
      if (!nodes_to_merge.hit_cycle) {
        // We need to update all the edges which have 
        // outgoing edges to end_terminal.
        EdgeTerminal old_terminal = end_terminal.flip();
        
        // The new terminal will be the reverse complement of the merged_strand.
        EdgeTerminal new_terminal = new EdgeTerminal(
            merged_node.getNodeId(), DNAStrandUtil.flip(merged_strand));
        
        moveIncomingEdges(nodes, old_terminal, new_terminal);      
        result.merged_nodeids.add(end_terminal.nodeId);
      } else {
        // We are merging a perfect cycle. 
        // e.g ATC->TCG->CGC->GCA->CAT
        // So the merged sequence is ATCGCAT
        // and has incoming/outgoing edges to itself. So we need to move
        // the incoming edge to ATC. E.g CAT->ATC needs to become
        // ATCGCAT -> ATCGCAT
        // CAT->ATC implies RC(ATC)->RC(CAT)
        // So we need to move the 
      
        DNAStrand strand = DNAStrandUtil.flip(merged_strand);
        EdgeTerminal old_terminal = end_terminal.flip();        
        EdgeTerminal new_terminal = new EdgeTerminal(
            merged_node.getNodeId(), strand);
        //moveOutgoingEdges(nodes, old_terminal, new_terminal);    
        //GraphNode source_node = nodes.get(source.nodeId);
        merged_node.moveOutgoingEdge(
            strand, old_terminal, new_terminal);
        result.merged_nodeids.add(end_terminal.nodeId);
      }
    }
                
    result.merged_node = merged_node;
    return result;
  }
}
