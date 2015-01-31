/**
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import contrail.stages.QuickMergeUtil;

/**
 * Represent a path in the graph.
 */
public class GraphPath {
  private static final Logger sLogger = Logger.getLogger(GraphPath.class);
  private HashMap<String, GraphNode> nodes;
  private ArrayList<EdgeTerminal> path;

  private HashSet<EdgeTerminal> terminalSet;

  public GraphPath() {
    nodes = new HashMap<String, GraphNode>();
    path = new ArrayList<EdgeTerminal>();
    terminalSet = new HashSet<EdgeTerminal>();
  }

  public void add(EdgeTerminal terminal, GraphNode node) {
    if (!terminal.nodeId.equals(node.getNodeId())) {
      sLogger.fatal(
          "The edgeterminal and the node don't match.",
          new RuntimeException("Invalid node and terminal pair."));
    }

    terminalSet.add(terminal);
    path.add(terminal);
    nodes.put(node.getNodeId(), node);
  }

  public void addToFront(EdgeTerminal terminal, GraphNode node) {
    if (!terminal.nodeId.equals(node.getNodeId())) {
      sLogger.fatal(
          "The edgeterminal and the node don't match.",
          new RuntimeException("Invalid node and terminal pair."));
    }

    terminalSet.add(terminal);
    path.add(0, terminal);
    nodes.put(node.getNodeId(), node);
  }

  @Override
  public int hashCode() {
    int hashCode = 0;
    for (EdgeTerminal terminal : path) {
      hashCode = hashCode ^ terminal.hashCode();
    }
    return hashCode;
  }

  /**
   * Add all the nodes in some other path to this one
   * @param other
   */
  public void addPath(GraphPath other) {
    for (EdgeTerminal terminal : other.path) {
      add(terminal, other.nodes.get(terminal.nodeId));
    }
  }

  /**
   * Check if two paths are the same.
   *
   * Note: This
   * @param other
   * @return
   */
  public boolean equals(Object o) {
    if (!(o instanceof GraphPath)) {
      return false;
    }
    GraphPath other = (GraphPath) o;

    if (path.size() != other.path.size()) {
      return false;
    }
    for (int pos = 0; pos < path.size(); ++pos) {
      if (!path.get(pos).equals(other.path.get(pos))) {
        return false;
      }
    }
    return true;
  }

  public EdgeTerminal last() {
    return path.get(path.size() - 1);
  }

  public EdgeTerminal first() {
    return path.get(0);
  }

  public GraphNode firstNode() {
    return nodes.get(first().nodeId);
  }

  public GraphNode lastNode() {
    return nodes.get(last().nodeId);
  }

  public GraphPath clone() {
    GraphPath newPath = new GraphPath();

    // Since edge terminals are immutable we can add them directly.
    newPath.path.addAll(path);

    // Clone the nodes.
    for (GraphNode node : nodes.values()) {
      newPath.nodes.put(node.getNodeId(), node.clone());
    }
    return newPath;
  }

  public String terminalNames() {
    String[] names = new String[path.size()];
    for (int i = 0; i < path.size(); ++i) {
      names[i] = path.get(i).toString();
    }
    return StringUtils.join(names, ",");
  }

  /**
   * Merge the nodes on the path into a single node. All edges except
   * those belonging to the path are pruned.
   * @return
   */
  public GraphNode merge(int K) {
    // We need to make a copy of the nodes without any edges except those
    // belong to the chain.
    HashMap<String, GraphNode> prunedNodes = new HashMap<String, GraphNode>();

    for (int i = 0; i< path.size(); ++i) {
      HashSet<String> neighborsToKeep = new HashSet<String>();
      if (i > 0) {
        neighborsToKeep.add(path.get(i - 1).nodeId);
      }
      if (i < path.size() - 1) {
        neighborsToKeep.add(path.get(i + 1).nodeId);
      }
      GraphNode node = nodes.get(path.get(i).nodeId).clone();
      for (String neighbor : node.getNeighborIds()) {
        if (!neighborsToKeep.contains(neighbor)) {
          node.removeNeighbor(neighbor);
        }
      }
      prunedNodes.put(node.getNodeId(), node);
    }

    QuickMergeUtil.NodesToMerge nodesToMerge =
        new QuickMergeUtil.NodesToMerge();
    nodesToMerge.start_terminal = first();
    nodesToMerge.end_terminal = last();
    nodesToMerge.direction = EdgeDirection.OUTGOING;
    QuickMergeUtil.ChainMergeResult result =
        QuickMergeUtil.mergeLinearChain(prunedNodes, nodesToMerge, K - 1);

    result.merged_node.setNodeId(terminalNames());
    return result.merged_node;
  }

  /**
   * @return: The length of the path.
   */
  public int length(int K) {
    if (path.size() == 0) {
      return 0;
    }
    // Initialize it to K - 1 because for the first node the first K -1 bases
    // haven't been counted yet.
    int length = K -1;
    for (EdgeTerminal terminal : path) {
      // We subtract K-1 bases because the first K - 1 bases overlap
      // with the previous node.
      length += nodes.get(terminal.nodeId).getSequence().size() - (K-1);
    }
    return length;
  }

  /**
   * Number of terminals in the path.
   * @return
   */
  public int numTerminals() {
    return path.size();
  }

  /**
   * Return the subpath consisting of terminals [i, j).
   */
  public GraphPath subPath(int i, int j) {
    GraphPath sub = new GraphPath();
    for (; i < j; ++i) {
      sub.add(path.get(i), nodes.get(path.get(i).nodeId));
    }
    return sub;
  }

  /**
   * @return: True if the path contains the indicated terminal.
   */
  public boolean contains(EdgeTerminal terminal) {
    return terminalSet.contains(terminal);
  }

  /**
   * Flip the path returns the path corresponding to the reverse complement.
   *
   * For example: if we have A->B->C, flip returns RC(C)->RC(B)->RC(A)
   */
  public GraphPath flip() {
    GraphPath flipped = new GraphPath();
    for (int pos = path.size() - 1; pos >= 0; --pos) {
      EdgeTerminal terminal = path.get(pos).flip();
      flipped.add(terminal, nodes.get(terminal.nodeId));
    }
    return flipped;
  }

  public String toString() {
    String[] terminals = new String[path.size()];
    for (int i = 0; i < path.size(); ++i) {
      terminals[i] = path.get(i).toString();
    }
    return StringUtils.join(terminals, "-");
  }
  
  public EdgeTerminal getTerminal(int i) {
    return path.get(i);
  }
}
