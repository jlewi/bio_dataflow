// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.graph;

import contrail.Node;
import contrail.sequences.DNAStrand;

/**
 * Routines for converting the new avro format to the old custom format.
 */
public class NodeConverter {
  /**
   * Convert an instance of the avro format to the old format.
   * @param graph_node
   * @return
   */
  public static Node graphNodeToNode(GraphNode graph_node) {
    Node node = new Node();
    node.setNodeId(graph_node.getNodeId());

    {
      GraphNodeKMerTag tag = graph_node.getData().getMertag();
      String mertag = tag.getReadTag().toString() + "_" + tag.getChunk();
      node.setMertag(mertag);
    }

    node.setCoverage(graph_node.getCoverage());
    node.setstr(graph_node.getSequence().toString());

    // Add the edges.
    for (NeighborData neighbor: graph_node.getData().getNeighbors()) {
      for (EdgeData edge_data: neighbor.getEdges()) {
        String strands_str = edge_data.getStrands().toString().toLowerCase();
        node.addEdge(strands_str, neighbor.getNodeId().toString());

        for (CharSequence read_tag : edge_data.getReadTags()) {
          node.addThread(
              strands_str, neighbor.getNodeId().toString(),
              read_tag.toString());
        }
      }
    }

    // Add the R5Tags.
    for (R5Tag tag: graph_node.getData().getR5Tags()) {
      int isRC = tag.getStrand() == DNAStrand.REVERSE ? 1 : 0;

      node.addR5(
          tag.getTag().toString(), tag.getOffset(), isRC,
          java.lang.Integer.MAX_VALUE);
    }
    return node;
  }
}
