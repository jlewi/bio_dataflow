package contrail.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import contrail.graph.EdgeData;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.NeighborData;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

/**
 * Some utilities for working with bubbles.
 *
 */
public class BubbleUtil {
  // Class is an instance class so we can use reuse storage  between calls.
  private HashSet<String> uniqueTags;
  HashSet<String> neighborIds;
  HashMap<StrandsForEdge, EdgeData> edgeDataMap =
      new HashMap<StrandsForEdge, EdgeData>();

  public BubbleUtil() {
    uniqueTags = new HashSet<String>();
    neighborIds = new HashSet<String>();
    edgeDataMap = new HashMap<StrandsForEdge, EdgeData>();
  }

  private void clearAll() {
    uniqueTags.clear();
    neighborIds.clear();
    edgeDataMap.clear();
  }

  /**
   * Ensures the read tags are unique.
   * @param edgeData
   */
  private void ensureUniqueTags(EdgeData edgeData) {
    // Make sure the readtags are unique.
    uniqueTags.clear();
    for (CharSequence tag : edgeData.getReadTags()) {
      uniqueTags.add(tag.toString());
    }
    edgeData.getReadTags().clear();
    edgeData.getReadTags().addAll(uniqueTags);
  }

  /**
   * Ensure all outgoing edges are to the forward strand of the palindrome.
   *
   * This function adjusts the edges in node so that all outgoing edges in
   * node to the palindrome to point to the forward strand of the palindrome.
   * @param node
   * @param palindrome
   */
  public void fixEdgesToPalindrom(GraphNode node, GraphNode palindrome) {
    clearAll();
    NeighborData neighbor = node.removeNeighbor(palindrome.getNodeId());

    for (EdgeData edgeData : neighbor.getEdges()) {
      StrandsForEdge strands = StrandsUtil.form(
          StrandsUtil.src(edgeData.getStrands()), DNAStrand.FORWARD);
      edgeData.setStrands(strands);

      if (!edgeDataMap.containsKey(strands)) {
        edgeDataMap.put(strands, edgeData);
      } else {
        edgeDataMap.get(strands).getReadTags().addAll(edgeData.getReadTags());
      }
    }

    neighbor.getEdges().clear();
    for (EdgeData edgeData : edgeDataMap.values()) {
      ensureUniqueTags(edgeData);
      neighbor.getEdges().add(edgeData);
    }
    node.addNeighbor(neighbor);
  }

  /**
   * Ensure all outgoing edges from a palindrome are from the forward strand.
   *
   * This function adjusts the edges in a palindrome so that all outgoing edges
   * are from the forward strand.
   * @param palindrome
   */
  public void fixEdgesFromPalindrome(GraphNode palindrome) {
    clearAll();
    // Find all neighbors of the palindrome with an outgoing edge from the
    // reverse strand of the palindrome.
    neighborIds.clear();
    List<EdgeTerminal> rTerminals =
        palindrome.getEdgeTerminals(DNAStrand.REVERSE, EdgeDirection.OUTGOING);
    for (EdgeTerminal terminal : rTerminals) {
      neighborIds.add(terminal.nodeId);
    }

    // When we call removeNeighbor matters because it forces GraphNode
    // to clear all derived data and we'd like to minimze the number of times
    // we recompute the derived data.
    ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();

    for (String id: neighborIds) {
      neighbors.add(palindrome.removeNeighbor(id));
    }

    // Loop over all the neighbors. Any edges which originate on the reverse
    // strand should be moved to the forward strand.
    HashMap<StrandsForEdge, EdgeData> edgeDataMap =
        new HashMap<StrandsForEdge, EdgeData>();

    for (NeighborData neighbor : neighbors) {
      edgeDataMap.clear();
      for (EdgeData edgeData : neighbor.getEdges()) {
        StrandsForEdge strands = StrandsUtil.form(
            DNAStrand.FORWARD, StrandsUtil.dest(edgeData.getStrands()));
        edgeData.setStrands(strands);
        if (!edgeDataMap.containsKey(strands)) {
          edgeDataMap.put(strands, edgeData);
        } else {
          edgeDataMap.get(strands).getReadTags().addAll(
              edgeData.getReadTags());
        }
      }
      neighbor.getEdges().clear();
      for (EdgeData edgeData : edgeDataMap.values()) {
        ensureUniqueTags(edgeData);
        neighbor.getEdges().add(edgeData);
      }
      // Add the neighbor back to the node.
      palindrome.addNeighbor(neighbor);
    }
  }
}
