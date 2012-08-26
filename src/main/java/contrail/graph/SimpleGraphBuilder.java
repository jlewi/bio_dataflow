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
  // TODO(jlewi): Add some testing for this class.
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
    // nodeid will be the canonical sequence.
    Sequence sequence = new Sequence(str_seq, alphabet);
    Sequence canonical_src = DNAUtil.canonicalseq(sequence);

    String nodeid = canonical_src.toString();
    return addNode(nodeid, str_seq);
  }

  /**
   * Add a node to the graph to represent this sequence. If
   * a node with this sequence exists throw an exception.
   *
   * @param sequence
   * @return nodeid for the new node.
   */
  public String addNode(String nodeid, String str_seq) {
    if (findEdgeTerminalForSequence(str_seq) != null) {
      throw new RuntimeException(
          "Can't add node for: " + str_seq + "because one already exists.");
    }

    Sequence sequence = new Sequence(str_seq, alphabet);
    Sequence canonical_src = DNAUtil.canonicalseq(sequence);

    GraphNode node = new GraphNode();
    node.setSequence(canonical_src);
    node.setNodeId(nodeid);
    nodes.put(nodeid, node);
    return nodeid;
  }

  /**
   * Finds the nodeid for the node representing this sequence.
   * Returns null if no node exists.
   * @param sequence
   */
  public String findNodeIdForSequence(String str_seq) {
    Sequence sequence = new Sequence(str_seq, alphabet);
    Sequence canonical_src = DNAUtil.canonicalseq(sequence);

    for (GraphNode node: nodes.values()) {
      if (node.getSequence().equals(canonical_src)) {
        return node.getNodeId();
      }
    }
    return null;
  }

  /**
   * Return the node for the particular sequence.
   * @param sequence
   */
  public GraphNode findNodeForSequence(String sequence) {
    String nodeID = findNodeIdForSequence(sequence);
    if (nodeID == null) {
      return null;
    }
    return nodes.get(nodeID);
  }

  /**
   * Finds the edge terminal for this sequence. Returns null if
   * there's no node in the graph representing this terminal
   * @param sequence
   */
  public EdgeTerminal findEdgeTerminalForSequence(String str_seq) {

    String nodeid = findNodeIdForSequence(str_seq);
    if (nodeid == null) {
      return null;
    }

    Sequence sequence = new Sequence(str_seq, alphabet);
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
   * Add an edge. Nodes for terminals must already exist.
   * @param src: Terminal for the src sequence.
   * @param dest: Terminal for the destination sequence.
   * @param overlap: The amount of overlap.
   */
  public void addEdge(EdgeTerminal src, EdgeTerminal dest, int overlap) {
    GraphNode src_node = nodes.get(src.nodeId);
    GraphNode dest_node = nodes.get(dest.nodeId);

    // Check the overlap
    Sequence src_sequence = src_node.getSequence();
    src_sequence = DNAUtil.sequenceToDir(src_sequence, src.strand);

    Sequence dest_sequence = dest_node.getSequence();
    dest_sequence = DNAUtil.sequenceToDir(dest_sequence, dest.strand);

    Sequence src_overlap = src_sequence.subSequence(
        src_sequence.size() - overlap, src_sequence.size());

    Sequence dest_overlap = dest_sequence.subSequence(
        0, overlap);

    if (!src_overlap.equals(dest_overlap)) {
        throw new RuntimeException("Sequences don't overlap by: " + overlap);
    }

    // Add the outgoing edge to src.
    {
      src_node.addOutgoingEdge(src.strand, dest);
    }

    // Add the incoming edge to dest.
    {
      dest_node.addIncomingEdge(dest.strand, src);
    }
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

    addEdge(src_terminal, dest_terminal, overlap);
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
