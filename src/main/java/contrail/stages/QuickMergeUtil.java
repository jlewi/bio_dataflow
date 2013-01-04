/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import contrail.graph.NodeMerger;
import contrail.graph.NodeMerger.MergeResult;
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
    NodeMerger merger = new NodeMerger();
    MergeResult mergeResult = merger.mergeNodes(
        mergedId, terminalsToMerge, nodes, overlap);

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

    result.merged_node = mergedNode;
    return result;
  }
}
