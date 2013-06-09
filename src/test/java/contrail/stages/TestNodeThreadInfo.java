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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphTestUtil;
import contrail.sequences.DNAStrand;

public class TestNodeThreadInfo {
  @Test
  public void testThreadable1() {
    // Test a node with two outgoing edges isn't threadable.
    GraphNode node = GraphTestUtil.createNode("node", "AAA");
    int maxThreads = 100;

    // Add two incoming edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in2", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);

    NodeThreadInfo threadInfo = new NodeThreadInfo(node);
    assertFalse(threadInfo.isThreadable());
  }

  @Test
  public void testThreadable2() {
    // Test a compressible node with spanning reads isn't threadable.
    GraphNode node = GraphTestUtil.createNode("node", "AAA");
    int maxThreads = 100;

    // Add two incoming edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);
    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);

    NodeThreadInfo threadInfo = new NodeThreadInfo(node);
    assertFalse(threadInfo.isThreadable());
  }

  @Test
  public void testThreadable3() {
    // Test a node with indegree 1 and outdegree 2 is threadable.
    GraphNode node = GraphTestUtil.createNode("node", "AAA");
    int maxThreads = 100;

    // Add two incoming edges.
    node.addIncomingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("in", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);
    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out1", DNAStrand.FORWARD),
        Arrays.asList("read1"), maxThreads);
    node.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("out2", DNAStrand.FORWARD),
        Arrays.asList("read2"), maxThreads);

    NodeThreadInfo threadInfo = new NodeThreadInfo(node);
    assertTrue(threadInfo.isThreadable());
  }
}
