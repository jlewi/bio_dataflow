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
 */
public class BubbleUtil {

  /**
   * Ensure all outgoing edges are to the forward strand of the palindrome.
   *
   * This function adjusts the edges in node so that all outgoing edges in
   * node to the palindrome to point to the forward strand of the palindrome.
   * @param node
   * @param palindrome
   */
  public static void fixEdgesToPalindrom(GraphNode node, GraphNode palindrome) {
    // Major strand has edges which need to be moved.
    // So get all the read trrayListags for edges to the palindrome.

    // Find the tags for the edges from node to the palindrome.
    HashMap<DNAStrand, HashSet<String>> allTags =
        new HashMap<DNAStrand, HashSet<String>>();

    allTags.put(DNAStrand.FORWARD, new HashSet<String>());
    allTags.put(DNAStrand.REVERSE, new HashSet<String>());

    // Which strands of node have an edge to the palindrom.
    HashSet<DNAStrand> strandsWithEdges = new HashSet<DNAStrand>();

    for (DNAStrand palindromeStrand : DNAStrand.values()) {
      EdgeTerminal palindromeTerminal = new EdgeTerminal(
          palindrome.getNodeId(), palindromeStrand);
      for (DNAStrand nodeStrand : DNAStrand.values()) {
        if (!node.getEdgeTerminalsSet(
            nodeStrand, EdgeDirection.OUTGOING).contains(
                palindromeTerminal)) {
          continue;
        }
        strandsWithEdges.add(nodeStrand);

        for (CharSequence tag :
          node.getTagsForEdge(nodeStrand, palindromeTerminal)) {
          allTags.get(nodeStrand).add(tag.toString());
        }
      }
    }
    // Remove the edges to the palindrome.
    node.removeNeighbor(palindrome.getNodeId());
    EdgeTerminal palindromeTerminal = new EdgeTerminal(
        palindrome.getNodeId(), DNAStrand.FORWARD);
    for (DNAStrand strand : strandsWithEdges) {
      HashSet<String> tags = allTags.get(strand);
      node.addOutgoingEdgeWithTags(
          strand, palindromeTerminal, tags, tags.size());
    }
  }

  /**
   * Ensure all outgoing edges from a palindrome are from the forward strand.
   *
   * This function adjusts the edges in a palindrome so that all outgoing edges
   * are from the forward strand.
   * @param palindrome
   */
  public static void fixEdgesFromPalindrome(GraphNode palindrome) {
    // Find all neighbors of the palindrome with an outgoing edge from the
    // reverse strand of the palindrome.
    HashSet<String> neighborIds = new HashSet<String>();
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

    HashSet<String> uniqueTags = new HashSet<String>();

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
        // Make sure the readtags are unique.
        uniqueTags.clear();
        for (CharSequence tag : edgeData.getReadTags()) {
          uniqueTags.add(tag.toString());
        }
        edgeData.getReadTags().clear();
        edgeData.getReadTags().addAll(uniqueTags);
        neighbor.getEdges().add(edgeData);
      }
      // Add the neighbor back to the node.
      palindrome.addNeighbor(neighbor);
    }
  }
}
