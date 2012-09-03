package contrail.stages;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import contrail.graph.EdgeData;
import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
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
  ArrayList<NeighborData> neighbors;

  public BubbleUtil() {
    uniqueTags = new HashSet<String>();
    neighborIds = new HashSet<String>();
    edgeDataMap = new HashMap<StrandsForEdge, EdgeData>();
    neighbors = new ArrayList<NeighborData>();
  }

  private void clearAll() {
    uniqueTags.clear();
    neighborIds.clear();
    edgeDataMap.clear();
    neighbors.clear();
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
   * This function adjusts the edges in node to a palindrome. The major
   * neighbor should have an outgoing strand to the forward strand of the
   * palindrome. The minor node should have an outgoing edge to the reverse
   * strand of the palindrome.
   *
   * @param node
   * @param palindromeId: Id of the palindrome.
   * @param isMajorNode: True if node is the major neighbor of the node.
   */
  public void fixEdgesToPalindrome(
      GraphNode node, String palindromeId, boolean isMajorNode) {
    clearAll();
    NeighborData neighbor = node.removeNeighbor(palindromeId);

    DNAStrand palindromeStrand = null;
    if (isMajorNode) {
      palindromeStrand = DNAStrand.FORWARD;
    } else {
      palindromeStrand = DNAStrand.REVERSE;
    }

    for (EdgeData edgeData : neighbor.getEdges()) {
      StrandsForEdge strands = StrandsUtil.form(
          StrandsUtil.src(edgeData.getStrands()), palindromeStrand);
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
   * Ensure palindrome has outgoing edges from a single strand to each node.
   *
   * Let P be a node corresponding to a palendrome and X another node
   * connected to P. There should only exist edge P->X or R(P)->X but not
   * both as this forms a bubble.
   *
   * This function assumes P has two neighbors and thus forms a potential
   * chain. To make the merge possible we ensure P is connected as follows
   * X->P->Y  where X is the majorId and Y is the minorId. This means P has
   * the following edges P->Y, RC(P)->X.
   *
   * are from the forward strand.
   * @param palindrome
   */
  public void fixEdgesFromPalindrome(GraphNode palindrome) {
    clearAll();
    if (palindrome.getNeighborIds().size() > 2 ){
      throw new RuntimeException(String.format(
          "The code assumes the palindrome has no more than two neighbors, " +
          "but the palindrome has %d neighbors.", neighbors.size()));
    }

    // Pop all the neighbors from the node.
    for (String id: palindrome.getNeighborIds()) {
      neighbors.add(palindrome.removeNeighbor(id));
    }

    String majorId;
    if (neighbors.size() == 2) {
      majorId = GraphUtil.computeMajorID(
        neighbors.get(0).getNodeId(), neighbors.get(1).getNodeId()).toString();
    } else {
      majorId = neighbors.get(0).getNodeId().toString();
    }
    // Loop over all the neighbors. Edges to the major node should
    // be moved so they are from the forward strand and edges to the minor node
    // should be from the forward strand.
    HashMap<StrandsForEdge, EdgeData> edgeDataMap =
        new HashMap<StrandsForEdge, EdgeData>();

    for (NeighborData neighbor : neighbors) {
      edgeDataMap.clear();
      DNAStrand srcStrand = null;
      if (neighbor.getNodeId().toString().equals(majorId)) {
        srcStrand = DNAStrand.REVERSE;
      } else {
        srcStrand = DNAStrand.FORWARD;
      }
      for (EdgeData edgeData : neighbor.getEdges()) {

        StrandsForEdge strands = StrandsUtil.form(
            srcStrand, StrandsUtil.dest(edgeData.getStrands()));
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
