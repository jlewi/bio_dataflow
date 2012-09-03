package contrail.stages;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.sequences.DNAStrand;

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
    HashSet<String> neighbors = new HashSet<String>();
    List<EdgeTerminal> rTerminals =
        palindrome.getEdgeTerminals(DNAStrand.REVERSE, EdgeDirection.OUTGOING);
    for (EdgeTerminal terminal : rTerminals) {
      neighbors.add(terminal.nodeId);
    }


    // Alltags contains the tags to the terminal for the given edge.
    HashMap<DNAStrand, HashSet<String>> allTags =
        new HashMap<DNAStrand, HashSet<String>>();
    allTags.put(DNAStrand.FORWARD, new HashSet<String>());
    allTags.put(DNAStrand.REVERSE, new HashSet<String>());

    HashSet<DNAStrand> strandsWithEdges = new HashSet<DNAStrand>();
    for (String neighborId : neighbors) {
      strandsWithEdges.clear();

      for (DNAStrand strand : DNAStrand.values()) {
        EdgeTerminal terminal = new EdgeTerminal(neighborId, strand);
        for (DNAStrand palindromeStrand : DNAStrand.values()) {
          if (!palindrome.getEdgeTerminalsSet(
                  palindromeStrand, EdgeDirection.OUTGOING).contains(terminal)) {
            continue;
          }
          strandsWithEdges.add(strand);
          for (CharSequence tag : palindrome.getTagsForEdge(strand, terminal)) {
            allTags.get(palindromeStrand).add(tag.toString());
          }
        }
      }
      palindrome.removeNeighbor(neighborId);
      for (DNAStrand strand : strandsWithEdges) {
        EdgeTerminal terminal = new EdgeTerminal(neighborId, strand);
        HashSet<String> tags = allTags.get(strand);
        palindrome.addOutgoingEdgeWithTags(
            DNAStrand.FORWARD, terminal, tags, tags.size());
      }
    }
  }
}
