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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.graph;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.junit.Test;

import contrail.io.IndexedRecordsInMemory;
import contrail.sequences.DNAStrand;

public class TestGraphBFSIterator {
  @Test
  public void testIterator() {
    // Construct a graph A->{B,C}
    // B->A
    // C->D.
    GraphNode nodeA = GraphTestUtil.createNode("A", "ACCT");
    GraphNode nodeB = GraphTestUtil.createNode("B", "ACCT");
    GraphNode nodeC = GraphTestUtil.createNode("C", "ACCT");
    GraphNode nodeD = GraphTestUtil.createNode("D", "ACCT");
    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeB, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeB, DNAStrand.FORWARD, nodeA, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeA, DNAStrand.FORWARD, nodeC, DNAStrand.FORWARD);
    GraphUtil.addBidirectionalEdge(
        nodeC, DNAStrand.FORWARD, nodeD, DNAStrand.FORWARD);

    IndexedRecordsInMemory<String, GraphNodeData> nodes =
        new IndexedRecordsInMemory<String, GraphNodeData>();

    nodes.put(nodeA.getNodeId(), nodeA.getData());
    nodes.put(nodeB.getNodeId(), nodeB.getData());
    nodes.put(nodeC.getNodeId(), nodeC.getData());
    nodes.put(nodeD.getNodeId(), nodeD.getData());

    IndexedGraph graph = new IndexedGraph(nodes);

    GraphBFSIterator iter = new GraphBFSIterator(graph, Arrays.asList("A"));

    // Keep track of the nodes at each hop.
    ArrayList<HashSet<String>> actual = new ArrayList<HashSet<String>>();
    for (int i = 0; i < 3; i++) {
      actual.add(new HashSet<String>());
    }
    while (iter.hasNext()) {
      GraphNode node = iter.next();
      int hop = iter.getHop();
      actual.get(hop).add(node.getNodeId());
    }

    HashSet<String> expected0 = new HashSet<String>();
    expected0.add("A");
    HashSet<String> expected1 = new HashSet<String>();
    expected1.addAll(Arrays.asList("B", "C"));
    HashSet<String> expected2 = new HashSet<String>();
    expected2.addAll(Arrays.asList("D"));

    assertEquals(expected0, actual.get(0));
  }
}
