package contrail.stages;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

public class TestPopBubblesAvro {

  class ReduceTestCase {
    public List<FindBubblesOutput> inputs;
    public GraphNodeData expectedOutput;
  }

  private ReduceTestCase createReduceTest () {
    ReduceTestCase testCase = new ReduceTestCase();
    // Create a test case in which a node has several edges removed.
    GraphNode node = new GraphNode();
    node.setNodeId("minor");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));
    node.setCoverage(1000);

    testCase.expectedOutput = node.clone().getData();

    float extraCoverage = 0;
    for (int i=0; i < 2; ++i) {
      EdgeTerminal terminal = new EdgeTerminal("pop" + i, DNAStrand.FORWARD);
      node.addOutgoingEdge(DNAStrand.FORWARD, terminal);

      FindBubblesOutput input = new FindBubblesOutput();
      input.setMinorNodeId(node.getNodeId());

      BubbleMinorMessage message = new BubbleMinorMessage();
      message.setExtraCoverage(10.0f * (i+1));
      input.setMinorMessages(new ArrayList<BubbleMinorMessage>());
      input.getMinorMessages().add(message);

      extraCoverage += message.getExtraCoverage();
      testCase.inputs.add(input);
    }

    float expectedCoverage =
    FindBubblesOutput nodeInput = new FindBubblesOutput();
    nodeInput.setNode(node.getData());
    testCase.inputs.add(nodeInput);

    return testCase;
  }
  @Test
  public void test() {
    fail("Not yet implemented");
  }

}
