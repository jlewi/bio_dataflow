package contrail.graph;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import contrail.sequences.DNAStrand;

/**
 * This class provides a walker that walks the graph starting at the node
 * provided (this node is not returned by the itertor). The walk continues as
 * long as 1) the node has outdegree(indegree) 1 (depending on the direction
 * of the walk), and 2) the next node is in the group of nodes passed as input
 * to the walker (this is necessary so the destination can be retrieved).
 * 3) the next node has indegree(outdegree) 1.
 *
 *  These constraints mean the walk is reversible; i.e starting at the last
 *  terminal we could walk backwards and hit the first terminal.
 *
 * If direction is OUTGOING then we walk the outgoing edges, so the nodes
 * returned will be
 * start_node -> c1 -> c2, ....
 * where c1 is the first node returned.
 *
 * If the direction is incoming nodes then we walk the incoming edges
 * starting at start_node so the nodes returned will be
 * ...-> c2 -> c1 -> start_node
 * where c1 is the first node returned.
 *
 * Cycles: Suppose we have the chain
 * start_node -> c1 -> c2 -> c3 -> c1-> c2 -> ...
 * The nodes returned will be,
 * c1, c2, c3
 * and hitCycle will return true. A cycle means we hit the same strand
 * of a node that we already visited. If you hit the same node but a different
 * strand then this is not considered to be a cycle.
 */
public class LinearChainWalker implements Iterator<EdgeTerminal> {

	private Map<String, GraphNode> nodes_in_memory;

	private EdgeDirection walk_direction;
	private EdgeTerminal current_terminal;

	private Boolean has_next;
	private EdgeTerminal next_terminal;

	// Keep track of the nodes we've already seen so we can detect cycles.
	// This will include the terminal at the start.
	private HashSet<EdgeTerminal> seen_terminals;

	// Keep track of whether we hit a cycle.
	private boolean hit_cycle;
	/**
	 * Initialize the walker.
	 * @param nodes_in_memory
	 * @param start
	 */
	private void init(
			Map<String, GraphNode> nodes_in_memory, EdgeTerminal start,
			EdgeDirection direction) {
		this.nodes_in_memory = nodes_in_memory;
		current_terminal = start;
		has_next = null;
		walk_direction = direction;
		seen_terminals = new HashSet<EdgeTerminal>();
		seen_terminals.add(start);
		hit_cycle = false;
	}

	/**
	 * Construct the walker.
	 * @param nodes_in_memory: A map containing the nodes keyed by node id
	 *   that we know about.
	 * @param start_node: The node where we start the walk. This node
	 *   will not be included in the walk.
	 * @param start_strand: Which strand of the start node to start on.
	 * @param walk_direction: Indicates in which direction to walk the graph
	 *   starting at start_node.
	 */
	public LinearChainWalker(
			Map<String, GraphNode> nodes_in_memory, GraphNode start_node,
			DNAStrand start_strand,
			EdgeDirection walk_direction) {
		init (nodes_in_memory,
			  new EdgeTerminal(start_node.getNodeId(), start_strand),
			  walk_direction);
	}

	/**
	 * Construct the walker.
	 * @param nodes_in_memory: A map containing the nodes keyed by node id
	 *   that we know about.
	 * @param start: The terminal to start on.
	 * @param start_strand: Which strand of the start node to start on.
	 * @param walk_direction: Indicates in which direction to walk the graph
	 *   starting at start_node.
	 */
	public LinearChainWalker(
			Map<String, GraphNode> nodes_in_memory, EdgeTerminal start,
			EdgeDirection walk_direction) {
		init (nodes_in_memory, start, walk_direction);
	}

	public boolean hasNext() {
		if (has_next == null) {
			// Check if we can continue the walk and cache the result.
			// Also cache the next node to return.
			has_next = false;

			GraphNode node = nodes_in_memory.get(current_terminal.nodeId);
			TailData tail = node.getTail(
					current_terminal.strand, this.walk_direction);
			if (tail != null) {
				if (nodes_in_memory.containsKey(tail.terminal.nodeId)) {
				  // Check if we've already seen this terminal. If we have then
				  // we hit a cycle and we stop.
				  if (seen_terminals.contains(tail.terminal)) {
				    has_next = false;
				    hit_cycle = true;
				  } else {
				    next_terminal = tail.terminal;
				    has_next = true;
				  }
				}
			}
		}
		// Return the cached value for has next
		return has_next.booleanValue();
	}

	public EdgeTerminal next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		// Advance current node.
		current_terminal = next_terminal;

		seen_terminals.add(current_terminal);
		// Clear the cache
		has_next = null;
		next_terminal = null;
		return current_terminal;
	}

	public void remove() {
		throw new UnsupportedOperationException(
				"Remove isn't supported for this iterator");
	}

	/**
	 * Returns true if we hit a cycle; i.e we stopped walking because
	 * we hit a terminal we already visited.
	 * @return
	 */
	public boolean hitCycle() {
	  return hit_cycle;
	}
}
