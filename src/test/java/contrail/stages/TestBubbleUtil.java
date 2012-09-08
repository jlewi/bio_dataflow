package contrail.stages;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

public class TestBubbleUtil {
  @Test
  public void testfixEdgesToPalindrome() {
    // Ensure that outgoing edges from a palindrome are fixed so that edge
    // to major node is from reverse strand and edge to minor node
    for (DNAStrand majorDestStrand : DNAStrand.values()) {
      for (DNAStrand minorDestStrand : DNAStrand.values()) {
        GraphNode palindrome = new GraphNode();
        palindrome.setNodeId("node");
        palindrome.setSequence(new Sequence(
            "AATT", DNAAlphabetFactory.create()));

        GraphNode majorNode = new GraphNode();
        majorNode.setSequence(new Sequence(
            "GCAA", DNAAlphabetFactory.create()));
        majorNode.setNodeId("B");

        GraphNode minorNode = new GraphNode();
        minorNode.setSequence(new Sequence(
            "GGAA", DNAAlphabetFactory.create()));
        minorNode.setNodeId("A");

        GraphUtil.addBidirectionalEdge(
            majorNode, DNAStrand.FORWARD, palindrome, majorDestStrand);
        GraphUtil.addBidirectionalEdge(
            minorNode, DNAStrand.FORWARD, palindrome, minorDestStrand);

        GraphNode expectedMajor = majorNode.clone();
        expectedMajor.removeNeighbor(palindrome.getNodeId());

        expectedMajor.addOutgoingEdge(
            DNAStrand.FORWARD,
            new EdgeTerminal(palindrome.getNodeId(), DNAStrand.FORWARD));


        GraphNode expectedMinor = minorNode.clone();
        expectedMinor.removeNeighbor(palindrome.getNodeId());

        expectedMinor.addOutgoingEdge(
            DNAStrand.FORWARD,
            new EdgeTerminal(palindrome.getNodeId(), DNAStrand.REVERSE));

        BubbleUtil bubbleUtil = new BubbleUtil();
        bubbleUtil.fixEdgesToPalindrome(
            majorNode, palindrome.getNodeId(), true);
        bubbleUtil.fixEdgesToPalindrome(
            minorNode, palindrome.getNodeId(), false);

        assertEquals(
            expectedMajor.getEdgeTerminalsSet(
                DNAStrand.FORWARD, EdgeDirection.OUTGOING),
            majorNode.getEdgeTerminalsSet(
                DNAStrand.FORWARD, EdgeDirection.OUTGOING));
        assertEquals(
            expectedMajor.getEdgeTerminalsSet(
                DNAStrand.REVERSE, EdgeDirection.OUTGOING),
            majorNode.getEdgeTerminalsSet(
                DNAStrand.REVERSE, EdgeDirection.OUTGOING));

        assertEquals(
            expectedMinor.getEdgeTerminalsSet(
                DNAStrand.FORWARD, EdgeDirection.OUTGOING),
            minorNode.getEdgeTerminalsSet(
                DNAStrand.FORWARD, EdgeDirection.OUTGOING));
        assertEquals(
            expectedMinor.getEdgeTerminalsSet(
                DNAStrand.REVERSE, EdgeDirection.OUTGOING),
            minorNode.getEdgeTerminalsSet(
                DNAStrand.REVERSE, EdgeDirection.OUTGOING));
      }
    }
  }

  @Test
  public void testfixEdgesFromPalindrome() {
    // Ensure that outgoing edges from a palindrome are fixed so that edge
    // to major node is from reverse strand and edge to minor node
    for (DNAStrand majorSrcStrand : DNAStrand.values()) {
      for (DNAStrand minorSrcStrand : DNAStrand.values()) {
        GraphNode palindrome = new GraphNode();
        palindrome.setNodeId("node");
        palindrome.setSequence(new Sequence(
            "AATT", DNAAlphabetFactory.create()));

        EdgeTerminal minorTerminal = new EdgeTerminal("A", DNAStrand.REVERSE);
        palindrome.addOutgoingEdge(minorSrcStrand, minorTerminal);

        EdgeTerminal majorTerminal = new EdgeTerminal("B", DNAStrand.FORWARD);
        palindrome.addOutgoingEdge(majorSrcStrand, majorTerminal);

        GraphNode expectedNode = palindrome.clone();
        expectedNode.removeNeighbor("A");
        expectedNode.removeNeighbor("B");
        expectedNode.addOutgoingEdge(DNAStrand.REVERSE, majorTerminal);
        expectedNode.addOutgoingEdge(DNAStrand.FORWARD, minorTerminal);

        BubbleUtil bubbleUtil = new BubbleUtil();
        bubbleUtil.fixEdgesFromPalindrome(palindrome);
        assertEquals(
            expectedNode.getEdgeTerminalsSet(
                DNAStrand.FORWARD, EdgeDirection.OUTGOING),
            palindrome.getEdgeTerminalsSet(
                DNAStrand.FORWARD, EdgeDirection.OUTGOING));
        assertEquals(
            expectedNode.getEdgeTerminalsSet(
                DNAStrand.REVERSE, EdgeDirection.OUTGOING),
            palindrome.getEdgeTerminalsSet(
                DNAStrand.REVERSE, EdgeDirection.OUTGOING));
      }
    }
  }
}
