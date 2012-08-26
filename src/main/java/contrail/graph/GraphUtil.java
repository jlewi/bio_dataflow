/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
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
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.graph;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * Some miscellaneous utilities for working with graphs.
 */
public class GraphUtil {
  /**
   * Order strings lexicographically and return the largest id.
   *
   * @param id1
   * @param id2
   * @return: The largest id.
   */
  public static CharSequence computeMajorID(
      CharSequence id1, CharSequence id2) {
    CharSequence major = "";
    if (id1.toString().compareTo(id2.toString()) > 0) {
      major = id1;
    } else  {
      major = id2;
    }
    return major;
  }

  /**
   * Order strings lexicographically and return the smallest id.
   *
   * @param id1
   * @param id2
   * @return: The smaller id.
   */
  public static CharSequence computeMinorID(
      CharSequence id1, CharSequence id2) {
    CharSequence minor = "";
    if (id1.toString().compareTo(id2.toString()) > 0) {
      minor = id2;
    } else  {
      minor = id1;
    }
    return minor;
  }

  /**
   * Write a list of a graph nodes to an avro.
   * @param avroFile
   * @param nodes
   */
  public static void writeGraphToFile(
      File avroFile, Collection<GraphNode> nodes) {
    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datumWriter =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (GraphNode node: nodes) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. " +
           "Exception: " + exception.getMessage());
    }
  }

  /**
   * Add a bidirectional edge.
   *
   * This functions adds the edge
   * (src, srcStrand)->(dest, destStrand) to src and the edge
   * (dest, R(destStrand))->(src, destStrand) to dest,
   * which ensures the graph is valid.
   */
  public static void addBidirectionalEdge(
      GraphNode src, DNAStrand srcStrand, GraphNode dest,
      DNAStrand destStrand) {
    src.addOutgoingEdge(
        srcStrand, new EdgeTerminal(dest.getNodeId(), destStrand));
    dest.addIncomingEdge(
        destStrand, new EdgeTerminal(src.getNodeId(), srcStrand));
  }

  /**
   * Validate a graph which fits in memory.
   *
   * For larger graphs use ValidateGraph.
   */
  public static List<GraphError> validateGraph(
      HashMap<String, GraphNode> nodes, int K) {
    ArrayList<GraphError> graphErrors = new ArrayList<GraphError>();
    // Iterate over the nodes.
    for (GraphNode node: nodes.values()) {
      // Iterate over all the edges
      for (DNAStrand strand: DNAStrand.values()) {
        Sequence sequence = DNAUtil.sequenceToDir(node.getSequence(), strand);
        Sequence srcOverlap = sequence.subSequence(
            sequence.size() - K + 1, sequence.size());
        for (EdgeTerminal terminal:
             node.getEdgeTerminals(strand, EdgeDirection.OUTGOING)) {
          if (!nodes.containsKey(terminal.nodeId)) {
            GraphError error = new GraphError();
            Formatter formatter = new Formatter(new StringBuilder());
            formatter.format(
                "There is an edge from %1$s:%2$s to %3$s:%4$s but there " +
                "is no node %3$s", node.getNodeId(), strand, terminal.nodeId,
                terminal.strand);

            error.setMessage(formatter.toString());
            error.setErrorCode(GraphErrorCodes.MISSING_NODE);
            graphErrors.add(error);
            continue;
          }
          GraphNode otherNode = nodes.get(terminal.nodeId);
          Sequence otherSequence = DNAUtil.sequenceToDir(
              otherNode.getSequence(), terminal.strand);
          Sequence otherOverlap = otherSequence.subSequence(0, K - 1);
          // Check the overlap.
          if (!srcOverlap.equals(otherOverlap)) {
            GraphError error = new GraphError();
            Formatter formatter = new Formatter(new StringBuilder());
            formatter.format(
                "There is an edge from %s:%s to %s:%s but the sequences " +
                "don't overlap", node.getNodeId(), strand, terminal.nodeId,
                terminal.strand);

            error.setMessage(formatter.toString());
            error.setErrorCode(GraphErrorCodes.OVERLAP);
            graphErrors.add(error);
          }

          // Check the dest has an outgoing edge to the node.
          List<EdgeTerminal> otherTerminals = otherNode.getEdgeTerminals(
              terminal.strand, EdgeDirection.INCOMING);
          EdgeTerminal expectedTerminal = new EdgeTerminal(
              node.getNodeId(), strand);
          boolean foundTerminal = false;
          for (EdgeTerminal possibleMatch : otherTerminals) {
            if (possibleMatch.equals(expectedTerminal)) {
              foundTerminal = true;
              break;
            }
          }
          if (!foundTerminal) {
            GraphError error = new GraphError();
            Formatter formatter = new Formatter(new StringBuilder());
            formatter.format(
                "There is an edge from %1$s:%2$s to %3$s:%4$s but %3$s:%4$s " +
                "is missing an incoming edge from %1$s:%2$s", node.getNodeId(),
                strand, terminal.nodeId, terminal.strand);

            error.setMessage(formatter.toString());
            error.setErrorCode(GraphErrorCodes.MISSING_EDGE);
            graphErrors.add(error);
          }
        }
      }
    }
    return graphErrors;
  }
}
