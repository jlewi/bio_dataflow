// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.LinearChainWalker;
import contrail.graph.NodeListMerger;
import contrail.graph.NodeListMerger.MergeResult;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;

/**
 * Some utilities for the QuickMergeStage.
 */
public class QuickMergeUtil {
  // TODO(jlewi): We should consider making this a non-static class;
  // NodeMerger. The advantage would be that the class could manage its storage
  // and could reuse objects rather than instantiating objects on each call.
  // For example, we could reuse a single instance of NodesWithEdgesToMove.
  /**
   * Data structure which provides information about the nodes which
   * can be merged.
   */
  public static class NodesToMerge {
    // The start and end terminals for the merge.
    public EdgeTerminal start_terminal;
    public EdgeTerminal end_terminal;

    // Direction for the merge.
    public EdgeDirection direction;

    // A set of all the nodes which we visited and processed. If these
    // nodes are part of any chain then that chain will be included
    // in the chain between start_terminal and end_terminal. We use this
    // set to determine which nodes can be eliminated from further
    // consideration.
    public HashSet<String> nodeids_visited;

    // Whether we hit a cycle or not
    public Boolean hit_cycle;
  }

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
    GraphNode node = nodes.get(terminal.nodeId);
    List<EdgeTerminal> incoming_terminals =
      node.getEdgeTerminals(terminal.strand, EdgeDirection.INCOMING);

    for (EdgeTerminal incoming_terminal: incoming_terminals) {
      if (!nodes.containsKey(incoming_terminal.nodeId)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Find a chain of nodes which can be merged.
   *
   * @param nodes_in_memory
   * @param start_node
   * @return: Instance of NodesToMerge.
   *
   * NodesToMerge.nodeids_visited is a list of all nodes visited, if any of
   * these nodes can be merged then they will be included in the chain between
   * start_terminal and end_terminal. Thus, we can eliminate these nodes
   * from further consideration when looking for chains.
   *
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
          return result;
        }
      } else {
        // There's no head so set the tail to start at the current node.
        // We might still have a chain node -> c1, c2, ... that we could
        // merge.
        head_terminal = new EdgeTerminal(
            start_node.getNodeId(), DNAStrand.FORWARD);
      }
    }

    start_node = nodes_in_memory.get(head_terminal.nodeId);

    TailData full_tail = TailData.findTail(
        nodes_in_memory, start_node,  head_terminal.strand,
        EdgeDirection.OUTGOING);

    if (full_tail == null || full_tail.dist == 0) {
      // No chain to merge.
      return result;
    }

    result.nodeids_visited.addAll(full_tail.nodes_in_tail);
    // We need to do some additional processing to determine
    // the actual ends of the chains.
    // Suppose we have x1->x2,...,->xn
    // We can include the ends x1, xn in this chain if the edges
    // y->x1 and z-> RC(xn) are in memory so they can be moved.
    boolean can_move_edges_to_start =
        checkIncomingEdgesAreInMemory(nodes_in_memory, head_terminal);

    boolean can_move_edges_to_end =
        checkIncomingEdgesAreInMemory(
            nodes_in_memory, full_tail.terminal.flip());

    if (full_tail.dist == 1) {
      // Consider the special case where there are no internal nodes in the
      // chain. In this special case we can only do the merge if we can move
      //  the edges to the nodes.
      // TODO(jlew): In certain special cases we can do the merge even
      // if the edges y->x1 or z->RC(x2) aren't in memory.
      // Suppose y isn't in memory but Id(x1) = Id(Merged(x1,x2)) and
      // Strand(x1) = Strand(Merged(x1,x2)). Then the terminal for y->x1
      // won't change so we don't have to move the edge y->x1 so the merge
      // could still proceed.
      if (!can_move_edges_to_start || !can_move_edges_to_end) {
        return result;
      }

      result.start_terminal = head_terminal;
      result.end_terminal = full_tail.terminal;
      return result;
    }

    if (can_move_edges_to_start) {
      result.start_terminal = head_terminal;
    } else {
      // Not all edge y->x1 are in memory so we have to start the merge
      // at x2.
      GraphNode node = nodes_in_memory.get(head_terminal.nodeId);
      TailData tail = node.getTail(
          head_terminal.strand, EdgeDirection.OUTGOING);
      result.start_terminal = tail.terminal;
    }

    if (can_move_edges_to_end) {
     result.end_terminal = full_tail.terminal;
    } else {
      GraphNode node = nodes_in_memory.get(full_tail.terminal.nodeId);
      TailData tail = node.getTail(
          full_tail.terminal.strand, EdgeDirection.INCOMING);
      result.end_terminal = tail.terminal;
    }

    // If start_terminal == end_terminal then we can't do any merges.
    // start_terminal might equal end_terminal because can_move_edges_to_start
    // and can_move_edges_to_end are false.
    if (result.start_terminal.equals(result.end_terminal)) {
      result.start_terminal = null;
      result.end_terminal = null;
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
   * @param excludeIds: List of nodes which should not have any edges
   *   moved because they are handled separately.
   */
  protected static void moveIncomingEdges(
      Map<String, GraphNode> nodes, EdgeTerminal terminal,
      EdgeTerminal new_terminal, HashSet<String> excludeIds) {

    GraphNode old_node = nodes.get(terminal.nodeId);
    List<EdgeTerminal> source_terminals = old_node.getEdgeTerminals(
        terminal.strand, EdgeDirection.INCOMING);

    for (EdgeTerminal source: source_terminals) {
      if (excludeIds.contains(source.nodeId)) {
        continue;
      }
      // We should already have checked that all incoming nodes are in the
      // map.
      GraphNode source_node = nodes.get(source.nodeId);
      source_node.moveOutgoingEdge(
          source.strand, terminal, new_terminal);
    }
  }

  /**
   * Move the edges in nodes which are connected to both ends of the merged
   * chain.
   *
   * Suppose we have the graph A->X->R(X)->R(A) and we are merging X & R(X).
   * In this case the forward and reverse strands are identical.
   * The merged sequence is  A->X'->R(A). In the original graph there is a
   * single outgoing edge in A to X; that is there is a single edge in A
   * corresponding to the outgoing edge from A to the start of the chain and
   * the incoming edge to R(A) from the end of the chain. However, in the
   * merged graph we have two edges  A->X' and A->R(X'). So
   * we can't simply move the incoming A->X to A->X'; we also need to create
   * a new edge to represent A->R(X').
   *
   * @param nodes: Nodes for the subgraph.
   * @param specialIds: The ids of nodes which have outgoing edges to the
   *   start of the merged chain and incoming edges from the end of the chain.
   * @param nodesToMerge
   * @param mergedNode
   * @param mergedStrand
   */
  protected static void moveEdgesToBothEnds(
      Map<String, GraphNode> nodes, HashSet<String> specialIds,
      NodesToMerge nodesToMerge, GraphNode mergedNode, DNAStrand mergedStrand) {
    // Process each of these nodes.
    for (String specialId: specialIds) {
      GraphNode node = nodes.get(specialId);

      DNAStrand startStrand = node.findStrandWithEdgeToTerminal(
          nodesToMerge.start_terminal, EdgeDirection.OUTGOING);

      // Collect the readtags associated with the edges so we can include
      // them when adding the new edges.
      ArrayList<CharSequence> startTags = new ArrayList<CharSequence>();
      for (CharSequence tag:
           node.getTagsForEdge(startStrand, nodesToMerge.start_terminal)) {
        // Convert it to a string so we make a copy of the data.
        startTags.add(tag.toString());
      }

      // This is the strand with an outgoing edge to the end terminal.
      EdgeTerminal endTerminal = nodesToMerge.end_terminal.flip();
      DNAStrand endStrand =  node.findStrandWithEdgeToTerminal(
          endTerminal, EdgeDirection.OUTGOING);

      // Collect the readtags associated with the edges so we can include
      // them when adding the new edges.
      ArrayList<CharSequence> endTags = new ArrayList<CharSequence>();
      for (CharSequence tag:
           node.getTagsForEdge(endStrand, endTerminal)) {
        // Convert it to a string so we make a copy of the data.
        endTags.add(tag.toString());
      }

      // Remove the neighbor data.
      node.removeNeighbor(nodesToMerge.start_terminal.nodeId);
      if (!nodesToMerge.start_terminal.nodeId.equals(
          nodesToMerge.end_terminal.nodeId)) {
        node.removeNeighbor(nodesToMerge.end_terminal.nodeId);
      }

      // Add the appropriate edges.
      final long MAXTHREADREADS = startTags.size() + endTags.size();
      EdgeTerminal newStart = new EdgeTerminal(
          mergedNode.getNodeId(), mergedStrand);
      node.addOutgoingEdgeWithTags(
          startStrand, newStart, startTags, MAXTHREADREADS);


      EdgeTerminal newEnd = new EdgeTerminal(
          mergedNode.getNodeId(), DNAStrandUtil.flip(mergedStrand));
      node.addOutgoingEdgeWithTags(
          endStrand, newEnd, endTags, MAXTHREADREADS);
    }
  }

  /**
   * Find nodes which have edges to both ends of the merged chain. These will
   * be nodes with outgoing edges to the start of the merged chain or nodes
   * with incoming edges from the end of the chain.
   *
   * @param nodes
   * @param mergedNode
   * @param mergedStrand
   */
  private static HashSet<String> findNodesWithEdgesToBothEnds(
      Map<String, GraphNode> nodes, NodesToMerge nodesToMerge) {
    HashSet<String> nodesToStart = new HashSet<String>();
    HashSet<String> nodesToBoth = new HashSet<String>();
    GraphNode startNode = nodes.get(nodesToMerge.start_terminal.nodeId);
    List<EdgeTerminal> sourceTerminals = startNode.getEdgeTerminals(
        nodesToMerge.start_terminal.strand, EdgeDirection.INCOMING);
    for (EdgeTerminal terminal: sourceTerminals) {
     nodesToStart.add(terminal.nodeId);
    }

    GraphNode endNode = nodes.get(nodesToMerge.end_terminal.nodeId);
    List<EdgeTerminal> destTerminals = endNode.getEdgeTerminals(
        nodesToMerge.end_terminal.strand, EdgeDirection.OUTGOING);

    for (EdgeTerminal terminal:destTerminals) {
      if (nodesToStart.contains(terminal.nodeId)) {
        nodesToBoth.add(terminal.nodeId);
      }
    }

    return nodesToBoth;
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
   * Merge together a set of nodes forming a chain.
   * @param nodes
   * @param nodes_to_merge
   * @param overlap
   * @return
   *
   * The graph after the merge is given by removing from nodes the merged nodes
   * as given by ChainMergeResult.merged_nodeids and then adding
   * ChainMergeResult.merged_node.
   */
  public static ChainMergeResult mergeLinearChain(
      Map<String, GraphNode> nodes, NodesToMerge nodes_to_merge, int overlap) {
    if (nodes_to_merge.direction != EdgeDirection.OUTGOING) {
      throw new NotImplementedException(
          "Currently we only support merging along the outgoing edges");
    }

    ChainMergeResult result = new ChainMergeResult();
    result.merged_nodeids = new HashSet<String>();

    // Initialize the merged_node to the node we start at.
//    GraphNode merged_node = nodes.get(nodes_to_merge.start_terminal.nodeId);
//
//    DNAStrand merged_strand = nodes_to_merge.start_terminal.strand;

    ArrayList<EdgeTerminal> terminalsToMerge = new ArrayList<EdgeTerminal>();

    LinearChainWalker walker = new LinearChainWalker(
        nodes, nodes_to_merge.start_terminal, EdgeDirection.OUTGOING);
    result.merged_nodeids.add(nodes_to_merge.start_terminal.nodeId);
    terminalsToMerge.add(nodes_to_merge.start_terminal);
    while (walker.hasNext()) {
      EdgeTerminal mergeTerminal = walker.next();
      terminalsToMerge.add(mergeTerminal);
      result.merged_nodeids.add(mergeTerminal.nodeId);
      if (mergeTerminal.equals(nodes_to_merge.end_terminal)) {
        break;
      }
    }

    String mergedId = nodes_to_merge.start_terminal.nodeId;
    NodeListMerger merger = new NodeListMerger();
    MergeResult mergeResult = merger.mergeNodes(
        mergedId, terminalsToMerge, nodes, overlap);

//    HashSet<String> nodesWithEdgesToBoth = findNodesWithEdgesToBothEnds(
//        nodes, nodes_to_merge);

    GraphNode mergedNode = mergeResult.node;
    DNAStrand mergedStrand = mergeResult.strand;

    // Identify all the nodes which have edges to the ends of the chain
    // but aren't part of the chain. We do this by adding the neighbors
    // of the start and end nodes and then removing any nodes that are
    // part of the chain.
    HashSet<String> externalNodes = new HashSet<String>();

    GraphNode startNode = nodes.get(nodes_to_merge.start_terminal.nodeId);
    GraphNode endNode = nodes.get(nodes_to_merge.end_terminal.nodeId);
    externalNodes.addAll(startNode.getNeighborIds());
    externalNodes.addAll(endNode.getNeighborIds());
    externalNodes.removeAll(result.merged_nodeids);

    // Now move all the edges.
    for (String externalId : externalNodes) {
      GraphNode externalNode = nodes.get(externalId);

      // In some edge cases a node might have edges to both ends of the
      // chain e.g the chain A->R(A) and the ends might be the same node.
      // So we copy the information needed for any new edges. We then
      // remove the neighbor and then add the edges.
      Set<DNAStrand> strandsToStart =
          externalNode.findStrandsWithEdgeToTerminal(
              nodes_to_merge.start_terminal, EdgeDirection.OUTGOING);

      HashMap<DNAStrand, List<CharSequence>> tagsToStart = null;

      if (strandsToStart.size() > 0) {
       tagsToStart = new HashMap<DNAStrand, List<CharSequence>>();
       for (DNAStrand strand : strandsToStart) {
         tagsToStart.put(
             strand, externalNode.getTagsForEdge(
                 strand, nodes_to_merge.start_terminal));
       }
      }

      Set<DNAStrand> strandsFromEnd =
          externalNode.findStrandsWithEdgeToTerminal(
              nodes_to_merge.end_terminal, EdgeDirection.INCOMING);

      HashMap<DNAStrand, List<CharSequence>> tagsFromEnd = null;

      if (strandsFromEnd.size() > 0) {
       tagsFromEnd = new HashMap<DNAStrand, List<CharSequence>>();
       for (DNAStrand strand : strandsFromEnd) {
         // We need to flip the edge because getTagsForEdge only works
         // for outgoing edges; so we need to get the tags for the equivalent
         // outgong edge.
         tagsFromEnd.put(
             strand, externalNode.getTagsForEdge(
                 DNAStrandUtil.flip(strand),
                 nodes_to_merge.end_terminal.flip()));
       }
      }

      // Remove the start and end terminals if they are currently
      // neighbors. We assume that GraphNode.removeNeighbor has no effect
      // if we remove a node which isn't a neighbor.
      externalNode.removeNeighbor(startNode.getNodeId());
      externalNode.removeNeighbor(endNode.getNodeId());

      // Add the edges to the merged node.
      EdgeTerminal mergedTerminal = new EdgeTerminal(
          mergedNode.getNodeId(), mergedStrand);

      for (DNAStrand strand : strandsToStart) {
        externalNode.addOutgoingEdgeWithTags(
            strand, mergedTerminal, tagsToStart.get(strand),
            tagsToStart.get(strand).size() + 1);
      }

      for (DNAStrand strand : strandsFromEnd) {
        externalNode.addIncomingEdgeWithTags(
            strand, mergedTerminal, tagsFromEnd.get(strand),
            tagsFromEnd.get(strand).size() + 1);
      }
    }



//    // Move nodes with edges to both ends of the chain.
//    moveEdgesToBothEnds(
//        nodes, nodesWithEdgesToBoth, nodes_to_merge, mergedNode,
//        mergedStrand);
//
//    // When moving the incoming edges to the ends of the chain we need
//    // to avoid cycles. We do this by adding the ids of the chain ends
//    // to the list of terminals to avoid merging.
//    nodesWithEdgesToBoth.add(nodes_to_merge.start_terminal.nodeId);
//    nodesWithEdgesToBoth.add(nodes_to_merge.end_terminal.nodeId);
//
//    // We need to move the incoming edges to the first node in the chain
//    // and the incoming edges to the last node in the chain.
//    {
//      // Move the edges y->x1
//      EdgeTerminal old_terminal = nodes_to_merge.start_terminal;
//      EdgeTerminal new_terminal = new EdgeTerminal(
//          mergedNode.getNodeId(), mergedStrand);
//
//      if (!old_terminal.equals(new_terminal)) {
//        moveIncomingEdges(
//            nodes, old_terminal, new_terminal, nodesWithEdgesToBoth);
//      }
//    }
//
//    {
//      // We need to move edges z->RC(xn)
//      EdgeTerminal old_terminal = nodes_to_merge.end_terminal.flip();
//      EdgeTerminal new_terminal = new EdgeTerminal(
//          mergedNode.getNodeId(), DNAStrandUtil.flip(mergedStrand));
//
//      // The nodeid for the merged node should be the same as the start
//      // terminal, but the strand might have changed. In which case we
//      // need to move the incoming edges
//      if (!old_terminal.equals(new_terminal)) {
//        moveIncomingEdges(
//            nodes, old_terminal, new_terminal, nodesWithEdgesToBoth);
//      }
    //}

    result.merged_node = mergedNode;
    return result;
  }
}
