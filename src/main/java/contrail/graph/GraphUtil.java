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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.tools.CreateGraphIndex;
import contrail.util.ContrailLogger;

/**
 * Some miscellaneous utilities for working with graphs.
 */
public class GraphUtil {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(CreateGraphIndex.class);
  /**
   * Check the suffix of src is a prefix of dest.
   * @param src
   * @param dest
   * @param nodes
   * @param overlap
   * @return
   */
  public static boolean checkOverlap(
      Sequence src, Sequence dest, int overlap) {
    Sequence srcSuffix = src.subSequence(src.size() - overlap, src.size());
    Sequence destPrefix = dest.subSequence(0, overlap);
    return srcSuffix.equals(destPrefix);
  }

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
   * Return the id of the major neighbor of the node.
   * @param node
   * @return
   */
  public static String computeMajorId(GraphNode node) {
    Iterator<String> iterator = node.getNeighborIds().iterator();
    String majorId =  iterator.next();
    while (iterator.hasNext()) {
      String next = iterator.next();
      if (next.compareTo(majorId) > 0) {
        majorId = next;
      }
    }
    return majorId;
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
      sLogger.fatal(
          "There was a problem writing the graph to an avro file. ",
          exception);
    }
  }

  /**
   * Write the nodes to the path.
   *
   * @param conf
   * @param path
   * @param nodes
   */
  public static void writeGraphToPath(
      Configuration conf, Path path, Iterable<GraphNode> nodes) {
    FileSystem fs = null;
    try{
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      sLogger.fatal("Can't get filesystem: " + e.getMessage(), e);
    }

    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datumWriter =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datumWriter);

    try {
      FSDataOutputStream outputStream = fs.create(path);
      writer.create(schema, outputStream);
      for (GraphNode node : nodes) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      sLogger.fatal(
          "There was a problem writing the N50 stats to an avro file. " +
          "Exception: " + exception.getMessage(), exception);
    }
  }

  /**
   * Write a list of a graph nodes to an indexed avro file.
   * @param avroFile
   * @param nodes
   */
  public static void writeGraphToIndexedFile(
      Configuration conf, Path outPath, List<GraphNode> nodes) {

    // Sort the nodes.
    Collections.sort(nodes, new GraphNode.NodeIdComparator());
    SortedKeyValueFile.Writer.Options writerOptions =
        new SortedKeyValueFile.Writer.Options();

    GraphNodeData nodeData = new GraphNodeData();
    writerOptions.withConfiguration(conf);
    writerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    writerOptions.withValueSchema(nodeData.getSchema());
    writerOptions.withPath(outPath);

    SortedKeyValueFile.Writer<CharSequence, GraphNodeData> writer = null;

    try {
      writer = new SortedKeyValueFile.Writer<CharSequence,GraphNodeData>(
          writerOptions);
    } catch (IOException e) {
      sLogger.fatal("There was a problem creating file:" + outPath, e);
    }

    for (GraphNode node : nodes) {
      try {
        writer.append(node.getNodeId(), node.getData());
      } catch (IOException e) {
        sLogger.fatal("Could not append the data.", e);
      }
    }

    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Could not close the file", e);
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

  /**
   * A comparator which can be used to sort nodes lexicogrphaically based on
   * node id.
   */
  public static class nodeIdComparator implements Comparator<GraphNode> {
    @Override
    public int compare(GraphNode left, GraphNode right) {
      return left.getNodeId().compareTo(right.getNodeId());
    }
  }
}
