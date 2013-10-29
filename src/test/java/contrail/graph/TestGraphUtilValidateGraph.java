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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import contrail.sequences.DNAStrand;

/**
 * Test GraphUtil.ValidateGraph works correctly.
 */
public class TestGraphUtilValidateGraph {
  @Test
  public void missingEdge() {
    // Construct a graph with missing edges.
    GraphNode nodeA = GraphTestUtil.createNode("A", "ACT");
    GraphNode nodeB = GraphTestUtil.createNode("B", "CTG");

    nodeA.addOutgoingEdge(
        DNAStrand.FORWARD, new EdgeTerminal("B", DNAStrand.FORWARD));

    int K = 3;
    List<GraphError> errors =
        GraphUtil.validateGraph(Arrays.asList(nodeA, nodeB), K);
    assertEquals(1, errors.size());
  }
}
