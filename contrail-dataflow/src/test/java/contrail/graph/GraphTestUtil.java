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

import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;

/**
 * Some utilities for working with graphs in unittest.
 */
public class GraphTestUtil {

  /**
   * Build a node with the given id and sequence.
   *
   * This is mostly useful for unittests where we don't want to use Sequences.
   *
   * @param nodeId
   * @param sequence
   */
  public static GraphNode createNode(String nodeId, String sequence) {
    // We keep this in the test package because actual code should use
    // Sequence and not string.
    GraphNode node = new GraphNode();
    node.setNodeId(nodeId);
    node.setSequence(new Sequence(sequence, DNAAlphabetFactory.create()));
    return node;
  }
}
