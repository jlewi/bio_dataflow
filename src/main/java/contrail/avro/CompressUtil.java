package contrail.avro;

import contrail.graph.GraphNode;
import contrail.sequences.DNAStrand;

/**
 * A collection of routines used by the stages for compressing linear chains.
 *
 */
public class CompressUtil {
  /**
   * Convert the enumeration CompressibleStrands to the equivalent DNAStrand
   * enumeration if possible.
   * @param strands
   * @return
   */
  protected static DNAStrand compressibleStrandsToDNAStrand(
      CompressibleStrands strands) {
    switch (strands) {
      case BOTH:
        return null;
      case NONE:
        return null;
      case FORWARD:
        return DNAStrand.FORWARD;
      case REVERSE:
        return DNAStrand.REVERSE;
      default:
        return null;
    }
  }

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

  /**
   * Convert DNAStrand to an instance of CompressibleStrands.
   */
  protected static CompressibleStrands dnaStrandToCompressibleStrands(
      DNAStrand strand) {
    switch (strand) {
      case FORWARD:
        return CompressibleStrands.FORWARD;
      case REVERSE:
        return CompressibleStrands.REVERSE;
      default:
        return null;
    }
  }
}
