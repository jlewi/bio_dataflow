package contrail.stages;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

public class TestBubbleUtil {
  @Test
  public void testfixEdgesFromPalindrome() {
    // Ensure that all outgoing edges from a palindrome are moved
    // to the outgoing edge.
    GraphNode palindrome = new GraphNode();
    palindrome.setNodeId("node");
    palindrome.setSequence(new Sequence("AATT", DNAAlphabetFactory.create()));

    EdgeTerminal fTerminal = new EdgeTerminal("A", DNAStrand.REVERSE);
    palindrome.addOutgoingEdge(DNAStrand.FORWARD, fTerminal);

    EdgeTerminal rTerminal = new EdgeTerminal("B", DNAStrand.FORWARD);
    palindrome.addOutgoingEdge(DNAStrand.REVERSE, rTerminal);

    GraphNode expectedNode = palindrome.clone();
    expectedNode.removeNeighbor("B");
    expectedNode.addOutgoingEdge(DNAStrand.FORWARD, rTerminal);

    BubbleUtil.fixEdgesFromPalindrome(palindrome);
    assertEquals(expectedNode.getData(), palindrome.getData());
  }
}
