package contrail.avro;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This class provides a walker that walks the graph starting at the node 
 * provided (the start node is not included). The walk continues as long as
 * 1) the node has outdegree or indegree 1 (depending on the direction of 
 * the walk), and 2) the next node is in the group of nodes passed as input
 * to the walker (this is necessary so the destination can be retrieved). 
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
 * The walk is always with respect to the sequence represented by
 * the canonical direction of start_node.
 */
public class LinearChainWalker implements Iterator<GraphNode> {
		
	private Map<String, GraphNode> nodes_in_memory;
	//private GraphNode start_node;
	private EdgeDirection walk_direction;
	private GraphNode current_node;
	
	private Boolean has_next;
	private GraphNode next_node; 	
	/**
	 * Construct the walker.
	 * @param nodes_in_memory: A map containing the nodes keyed by node id
	 *   that we know about. 
	 * @param start_node: The node where we start the walk. This node
	 *   will not be included in the walk.
	 * @param walk_direction: Indicates in which direction to walk the graph
	 *   starting at start_node. 
	 */
	public LinearChainWalker(
			Map<String, GraphNode> nodes_in_memory, GraphNode start_node,
			EdgeDirection walk_direction) {
		this.nodes_in_memory = nodes_in_memory;
		this.current_node = start_node;
		this.walk_direction = walk_direction;
		next_node = null;
		has_next = null;
	}
	
	public boolean hasNext() {		
		if (has_next == null) {
			// Check if we can continue the walk and cache the result.
			// Also cache the next node to return.
			has_next = false;
			String node_dir = "f";
			if (walk_direction == EdgeDirection.OUTGOING) {
				node_dir = "f";
			} else {
				node_dir = "r";
			}
			TailInfoAvro tail = this.current_node.getTail(node_dir);
			if (tail != null) {
				if (nodes_in_memory.containsKey(tail.id)) {
					next_node = nodes_in_memory.get(tail.id);
					has_next = true;
				}
			}
		}
		// Return the cached value for has next
		return has_next.booleanValue();
	}
	
	public GraphNode next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		// Advance current node.
		current_node = next_node;
		
		// Clear the cache
		has_next = null;
		next_node = null;
		return current_node;
	}

	public void remove() {
		throw new UnsupportedOperationException(
				"Remove isn't supported for this iterator");
	}
}
