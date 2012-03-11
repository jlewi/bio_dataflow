package contrail.graph;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * Simple in memory graph builder/manipulator.
 * 
 * This class is mostly useful for the unittests. For constructing actual
 * graphs we use map reduce (BuildGraphAvro). This class isn't efficient
 * by any means.
 */
public class SimpleGraphBuilder {
  // TODO(jlewi): Add some testing.
  private Hashtable<String, GraphNode> nodes;
  private Alphabet alphabet;
  public SimpleGraphBuilder() {
    nodes = new Hashtable<String, GraphNode> ();
    alphabet = DNAAlphabetFactory.create();
  }
    
  /**
   * Add a node to the graph to represent this sequence. If
   * the node already exists throw an exception.
   * 
   * @param sequence
   * @return nodeid for the new node.
   */
  public String addNode(String str_seq) {
    if (findEdgeTerminalForSequence(str_seq) != null) {
      throw new RuntimeException(
          "Can't add node for: " + str_seq + "because one already exists.");
    }
        
    Sequence sequence = new Sequence(str_seq, alphabet);
    Sequence canonical_src = DNAUtil.canonicalseq(sequence);
    
    String nodeid = canonical_src.toString();
    GraphNode node = new GraphNode(); 
    node.setCanonicalSequence(canonical_src);
    node.setNodeId(nodeid);
    nodes.put(nodeid, node);  
    return nodeid;    
  }
  
  /**
   * Finds the edge terminal for this sequence. Returns null if 
   * there's no node in the graph representing this terminal
   * @param sequence
   */
  public EdgeTerminal findEdgeTerminalForSequence(String str_seq) {
    Sequence sequence = new Sequence(str_seq, alphabet);
    Sequence canonical_src = DNAUtil.canonicalseq(sequence);
    
    String nodeid = canonical_src.toString();
    GraphNode node = nodes.get(nodeid);
    
    if (node == null) {
      return null;
    }
    
    EdgeTerminal terminal = new EdgeTerminal(
        nodeid, DNAUtil.canonicaldir(sequence));
    return terminal;    
  }
  
  public GraphNode getNode(String nodeid) {
    return nodes.get(nodeid);
  }
  
  /**
   * Returns an unmodifiable view into the set of nodes owned by
   * this graph. Note you can still modify the individual nodes in the graph.
   * @return
   */
  public Map<String, GraphNode> getAllNodes() {
    return Collections.unmodifiableMap(nodes);    
  }
  /**
   * Add an edge.
   * @param src: String representing the source sequence.
   * @param dest: String representing the destination sequence.
   * @param overlap: The amount of overlap.
   */
  public void addEdge(String src, String dest, int overlap) {
    if (!src.substring(src.length() - overlap).equals(
        dest.substring(0, overlap))) {
        throw new RuntimeException("Sequences don't overlap by: " + overlap);
     }
    
    EdgeTerminal src_terminal = findEdgeTerminalForSequence(src);
    if (src_terminal == null) {
        addNode(src);
        src_terminal = findEdgeTerminalForSequence(src);
    }
    EdgeTerminal dest_terminal = findEdgeTerminalForSequence(dest);
    if (dest_terminal == null) {
        addNode(dest);
        dest_terminal = findEdgeTerminalForSequence(dest);
    }
    
    // Add the outgoing edge to src.
    {
      GraphNode node = nodes.get(src_terminal.nodeId);
      node.addOutgoingEdge(src_terminal.strand, dest_terminal);
    }
    
    // Add the incoming edge to dest.
    {
      GraphNode node = nodes.get(dest_terminal.nodeId);
      node.addIncomingEdge(dest_terminal.strand, src_terminal);
    }
  }
  
  /**
   * Divide the string into kmers of length given by k and add the appropriate
   * edges.
   */
  public void addKMersForString(String str_seq, int k) {
    int start = 0;
    while (start + k < str_seq.length()) {
      String src = str_seq.substring(start, start + k);
      String dest = str_seq.substring(start + 1, start + k + 1);
      
      addEdge(src, dest, k - 1);
      start += 1;
    }
  }
}
