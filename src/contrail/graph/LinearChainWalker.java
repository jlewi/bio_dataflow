package contrail.graph;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import contrail.avro.EdgeDirection;
import contrail.avro.GraphNode;
import contrail.avro.TailInfoAvro;
import contrail.sequences.DNAStrand;

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

	private EdgeDirection walk_direction;
	private GraphNode current_node;
	private DNAStrand current_strand;
	
	private Boolean has_next;
	private GraphNode next_node; 	
	private DNAStrand next_strand;
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
		this.nodes_in_memory = nodes_in_memory;
		this.current_node = start_node;
		this.current_strand = start_strand;
		this.walk_direction = walk_direction;
		next_node = null;
		next_strand = null;
		has_next = null;
	}
	
	public boolean hasNext() {		
		if (has_next == null) {
			// Check if we can continue the walk and cache the result.
			// Also cache the next node to return.
			has_next = false;
			
			
			TailInfoAvro tail = this.current_node.getTail(
					current_strand, this.walk_direction);
			if (tail != null) {
				if (nodes_in_memory.containsKey(tail.terminal.nodeId)) {
					next_node = nodes_in_memory.get(tail.terminal.nodeId);
					next_strand = tail.terminal.strand;
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
		current_strand = next_strand;
		
		// Clear the cache
		has_next = null;
		next_node = null;
		next_strand = null;
		return current_node;
	}

	public void remove() {
		throw new UnsupportedOperationException(
				"Remove isn't supported for this iterator");
	}
}
