package contrail.graph;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.junit.Test;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

public class TestNodeListMerger extends NodeListMerger {
  @Test
  public void testMergeSequences() {
    // Create a random sequences.
    int overlap = 13;
    int numNodes = 23;
    Random generator = new Random();

    // Create a chain. We do this by generating numNodes random sequences.
    // We then append the last overlap characters from the previous node
    // to the start of the next node.
    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
    ArrayList<EdgeTerminal> terminals = new ArrayList<EdgeTerminal>();

    HashMap<String, GraphNode> nodesMap = new HashMap<String, GraphNode>();
    String trueSequence = "";
    for (int nindex = 0; nindex < numNodes; ++nindex) {
      GraphNode newNode = new GraphNode();
      newNode.setNodeId("node" + nindex);
      String dna = "";

      if (nindex > 0) {
        Sequence previousSequence = nodes.get(nindex - 1).getSequence();
        EdgeTerminal previousTerminal = terminals.get(nindex - 1);
        previousSequence = DNAUtil.sequenceToDir(
            previousSequence, previousTerminal.strand);
        dna = previousSequence.toString().substring(
            previousSequence.size() - overlap, previousSequence.size());
      }

      String newDna = AlphabetUtil.randomString(
          generator, overlap + 1, DNAAlphabetFactory.create());

      trueSequence += newDna;
      dna += newDna;


      Sequence sequence = new Sequence(dna, DNAAlphabetFactory.create());
      GraphNode node =  new GraphNode();
      node.setNodeId("node" + nindex);
      node.setSequence(DNAUtil.canonicalseq(sequence));

      nodes.add(node);
      nodesMap.put(node.getNodeId(), node);
      EdgeTerminal terminal = new EdgeTerminal(
          node.getNodeId(), DNAUtil.canonicaldir(sequence));
      terminals.add(terminal);
    }

    NodeListMerger merger = new NodeListMerger();

    Sequence mergedSequence = merger.mergeSequences(terminals, nodesMap, overlap);

    assertEquals(trueSequence, mergedSequence.toString());
  }
}
