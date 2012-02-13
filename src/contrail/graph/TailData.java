package contrail.graph;

import contrail.avro.GraphNode;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
	public int    dist;
	
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
	}
	
	public TailData()
	{
		terminal = null;
		dist = 0;
	}

	/**
	 * Check if a set of nodes contains a tail terminating on startnode.
	 * 
	 * A tail is a sequence of nodes which have outdegree 1. If a subset of the nodes
	 * in nodes forms a tail terminating on startnode, then we can compress these nodes
	 * because there's only one path through the nodes.
	 * 
	 * @param nodes - A map of nodes. We can only compress a chain terminating on startnode
	 *   if the nodes startnode is connected to are stored in this map. The keys of the map 
	 *   are the node ids and the values are the actual nodes.   
	 * @param startnode - Node where we begin our search for the tail. 
	 * @param startdir - The direction of the sequence in startnode to consider. 
	 * @return An instance of TailInfo describing the tail/chain. The id of the 
	 *   tail will be the last node in the chain; startnode is the other end. 
	 *   dist will be the number of edges spanned by the chain.
	 * @throws IOException
	 */
	public static TailData find_tail(
	    Map<String, GraphNode> nodes, GraphNode startnode, DNAStrand startdir) 
	        throws IOException {		
		//System.err.println("find_tail: " + startnode.getNodeId() + " " + startdir);
		Set<String> seen = new HashSet<String>();
		seen.add(startnode.getNodeId());
		
		GraphNode curnode = startnode;
		DNAStrand curdir = startdir;
		String curid = startnode.getNodeId();
		int dist = 0;
		
		boolean canCompress = false;
		
		do
		{
			canCompress = false;			
			TailData next = curnode.getTail(curdir);
			
			//System.err.println(curnode.getNodeId() + " " + curdir + ": " + next);

			if ((next != null) &&
				(nodes.containsKey(next.id)) &&
				(!seen.contains(next.id)))
			{
				// curnode has a tail (has outgoing degree 1); the tail 
				// is in nodes and we haven't seen it before
				
				seen.add(next.id.toString());
				curnode = nodes.get(next.id);

				// We can only compress the tail if the tail has a single incoming edge.
				// To check whether curnode has a single incoming edge, we look
				// for outgoing edges with the direction for the source flipped; this
				// produces a list of incoming edges. If there is a single incoming
				// edge then there is only a single path between the nodes (i.e they
				// form a chain with no branching and we can compress the nodes together)
				TailData nexttail = curnode.getTail(next.strand.flip());
				
				if ((nexttail != null) && (nexttail.id.equals(curid)))
				{
					dist++;
					canCompress = true;					
					curid = next.id.toString();
					curdir = next.strand;
				}
			}
		}
		while (canCompress);

		TailData retval = new TailData();
		
		retval.id = curid;
		// JLEWI: This is an abuse of dir; it shouldn't be used
		// to represent both the direction of the tail node and the tail 
		// direction. 
		retval.strand = curdir.flip();
		retval.dist = dist;
			
		return retval;
	}
	
}
