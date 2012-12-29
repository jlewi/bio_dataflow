/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import contrail.graph.GraphNode;
import contrail.sequences.DNAStrand;

/**
 * A collection of routines used by the stages for compressing linear chains.
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
   * Note: Eventually we will just use AVRO methods once the following issue
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
   * Make a deep copy of NodeInfoForMerge.
   *
   * Note: Eventually we will just use AVRO methods once the following issue
   * is resolved.
   * https://issues.apache.org/jira/browse/AVRO-1045.
   * @param node_info
   * @return
   */
  public static NodeInfoForMerge copyNodeInfoForMerge(
      NodeInfoForMerge node_info) {
    NodeInfoForMerge copy = new NodeInfoForMerge();
    copy.setStrandToMerge(node_info.getStrandToMerge());
    copy.setCompressibleNode(
        copyCompressibleNode(node_info.getCompressibleNode()));
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
