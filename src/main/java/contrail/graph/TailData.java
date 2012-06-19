package contrail.graph;

import contrail.sequences.DNAStrand;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

/**
 * This class is used to represent a tail. We get a tail when
 * a node has out degree 1.
 *
 */
public class TailData
{
	/**
	 * Identifies the terminal for the edge. This is always the terminal
	 * furtherest from the node we start from when finding the tail.
	 */
	public EdgeTerminal terminal;

	/**
	 * Which direction we walked from the start node. So you reverse
	 * this direction to get to the start node.
	 */
	public EdgeDirection direction;

	/**
	 * dist is the number of edges this tail spans.
	 */
	public int dist;

	// Set of all the nodes in the tail; this is an inclusive
	// list of all nodes from [head, end] where head is the terminal
	// we start the walk at and end is the last terminal in the walk.
	public HashSet<String> nodes_in_tail;

	// Whether or not the tail forms a cycle.
	// A cycle is defined as a sequence of terminals with indegree 1 and
	// outdegree 1 which are connected together. Thus, if you follow
	// outgoing edges or incoming edges you cycle around forever.
	public boolean hit_cycle;

	/**
	 * Copy Constructor.
	 *
	 * @param o
	 */
	public TailData(TailData o)
	{
	  terminal = o.terminal;
	  direction = o.direction;
	  dist = o.dist;
	  nodes_in_tail = new HashSet<String>();
	  nodes_in_tail.addAll(o.nodes_in_tail);
	}

	public TailData()
	{
		terminal = null;
		dist = 0;
    nodes_in_tail = new HashSet<String>();
	}

	/**
	 * Find a tail.
	 *
	 * A tail is a sequence of nodes with degree 1. We can only follow the
	 * tail if the nodes are in memory.
	 *
	 * @param nodes: A map of nodes. We can only follow a chain if the nodes
	 *   are stored in this map. The keys of the map
	 *   are the node ids and the values are the actual nodes.
	 * @param startnode: Node where we begin our search for the tail.
	 * @param start_strand: Which strand in node to begin on.
	 * @param direction: Which direction to find the tail in.
	 * @return An instance of TailDaita describing the tail/chain. The
	 *   last terminal in the chain is given by terminal.
	 *   dist will be the number of edges spanned by the chain.
	 * @throws IOException
	 */
	public static TailData findTail(
	    Map<String, GraphNode> nodes, GraphNode startnode,
	    DNAStrand start_strand, EdgeDirection direction) {

		String curid = startnode.getNodeId();
		int dist = 0;

		EdgeTerminal previous_terminal = new EdgeTerminal(
				startnode.getNodeId(), start_strand);
		LinearChainWalker walker = new LinearChainWalker(
				nodes, previous_terminal, direction);

		TailData tail = new TailData();
		tail.nodes_in_tail.add(curid);
		while(walker.hasNext()) {
			// The walker only returns a terminal if the corresponding
			// GraphNode is in the map passed to LinearChainWalker, so
		  // we don't have to check if the node is present.
			EdgeTerminal terminal = walker.next();

			// Suppose node A has out degree 1 and A->B.
			// B, however, might have indegree 2; i.e there exists edge
			// C->B. In this case, we don't have  a tail. So
			// We need to check that the current terminal corresponds to
			// an edge with degree in the direction opposite walk direction.
			// If there is a single edge then there is a single path between
			// the nodes (i.e they form a chain with no branching and we
			// can compress the nodes together)

			// Get the graph node associated with terminal and check
			// it has a single edge connected to previous_terminal.
			// TODO(jlewi): This check should be unnecessary because
			// LinearChainWalker is already doing it.
			GraphNode node = nodes.get(terminal.nodeId);
			TailData end_tail = node.getTail(
					terminal.strand, EdgeDirectionUtil.flip(direction));
			if ((end_tail != null) &&
				(end_tail.terminal.equals(previous_terminal))) {
				dist++;
	      // curnode has a tail (has outgoing degree 1); the tail
	      // is in nodes and we haven't seen it before
	      tail.nodes_in_tail.add(terminal.nodeId);
			} else {
				// Break out out of the loop because we don't have
				// a chain.
				break;
			}

			previous_terminal = terminal;
		}

		if (dist == 0) {
			return null;
		}

		tail.terminal = previous_terminal;
		tail.dist = dist;
		tail.direction = direction;
		tail.hit_cycle = walker.hitCycle();
		return tail;
	}
}
