package contrail.avro;

import contrail.graph.GraphNode;

/**
 * A collection of routines used by the stages for compressing linear chains.
 *
 */
public class CompressUtil {
  /**
   * Make a copy of a CompressibleNodeData record.
   *
   * Note: Eventually we will just use Avro methods once the following issue
   * is resolved.
   * https://issues.apache.org/jira/browse/AVRO-1045.
   * @param node
   * @return
   */
  public static CompressibleNodeData copyCompressibleNode(
      CompressibleNodeData node) {
    CompressibleNodeData copy = new CompressibleNodeData();
    copy.setCompressibleStrands(node.getCompressibleStrands());
    copy.setNode((new GraphNode(node.getNode())).clone().getData());
    return copy;
  }
}
