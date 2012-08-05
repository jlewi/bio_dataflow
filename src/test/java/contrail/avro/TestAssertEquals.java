package contrail.avro;

import static org.junit.Assert.*;

import org.junit.Test;

import contrail.RemoveTipMessage;
import contrail.graph.GraphNode;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;

public class TestAssertEquals {

  @Test
  public void test() {
    // Don't commit this code to the main repo. It was for a oneoff test.
    GraphNode node = new GraphNode();
    node.setNodeId("node");
    node.setSequence(new Sequence("ACT", DNAAlphabetFactory.create()));
    RemoveTipMessage msg1 = new RemoveTipMessage();
    msg1.setEdgeStrands(StrandsForEdge.FF);
    msg1.setNode(node.clone().getData());

    RemoveTipMessage msg2 = new RemoveTipMessage();
    msg2.setEdgeStrands(StrandsForEdge.FR);
    msg2.setNode(node.clone().getData());

    assertEquals(msg1, msg2);

  }
}
