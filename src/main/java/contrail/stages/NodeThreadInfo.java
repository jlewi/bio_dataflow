/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.sequences.DNAStrand;

/**
 * Utility class providing information about a nodes threads.
 */
public class NodeThreadInfo {
  final public HashMap<String, EdgeTerminal> inReads;
  final public HashMap<String, EdgeTerminal> outReads;

  /**
   * Ids of reads which span the node.
   */
  final public Set<String> spanningIds;

  // Has reads which span the node can can be used to disambiguate
  // edges.
  private boolean threadable;

  public NodeThreadInfo(GraphNode node) {
    // Get a list of reads for incoming edges.
    inReads = new HashMap<String, EdgeTerminal>();
    outReads = new HashMap<String, EdgeTerminal>();
    // Get outgoing edges for the forward strand.
    for (EdgeTerminal terminal : node.getEdgeTerminalsSet(
        DNAStrand.FORWARD, EdgeDirection.OUTGOING)) {
      for (CharSequence read :
           node.getTagsForEdge(DNAStrand.FORWARD, terminal)) {
        outReads.put(read.toString(), terminal);
      }
    }

    // To get reads for incoming edges we look at outgoing edges for the
    // reverse strand. getTagsForEdge assumes the edge is an outgoing edge.
    for (EdgeTerminal terminal : node.getEdgeTerminalsSet(
        DNAStrand.REVERSE, EdgeDirection.OUTGOING)) {
      for (CharSequence read :
           node.getTagsForEdge(DNAStrand.REVERSE, terminal)) {
       inReads.put(read.toString(), terminal);
      }
    }

    Set<String> spanningIdsTemp = inReads.keySet();
    spanningIdsTemp.retainAll(outReads.keySet());
    spanningIds = Collections.unmodifiableSet(spanningIdsTemp);

    // For the node to be threadable it must meet the following criterion:
    // 1. Incoming and outgoing degree > 0
    // 2. Incoming or outgoing degree > 1
    // 3. At least 1 read which spans the incoming and outgoing edges.
    if (spanningIds.size() == 0) {
      threadable = false;
    } else {
      if (node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING) == 0 ||
          node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING) == 0) {
        threadable = false;
      } else {
        if (node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING) <= 1 &&
            node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING) <= 1) {
          threadable = false;
        } else {
          threadable = true;
        }
      }
    }
  }

  public boolean isThreadable() {
    return threadable;
  }
}
