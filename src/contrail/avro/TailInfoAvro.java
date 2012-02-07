package contrail.avro;

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
public class TailInfoAvro 
{
	/**
	 * id identifies the second node in the edge. 
	 * 
	 * Node.gettail sets the id to the compressed k-mer representation of the
	 * destination node for the edge.
	 */
	public CharSequence id;
	/**
	 * dir is the direction in which the destination node k-mer is oriented. Can be either
	 * "r" or "f".
	 */
	public CharSequence dir;
	/**
	 * dist is the number of edges this tail spans. 
	 */
	public int    dist;
	
	/**
	 * Copy Constructor.
	 * 
	 * @param o
	 */
	public TailInfoAvro(TailInfoAvro o)
	{
	  throw new RuntimeException("Class needs to be updated to use datastructures used in avro version of contrail.");
//		id   = o.id;
//		dir  = o.dir;
//		dist = o.dist;
	}
	
	public TailInfoAvro()
	{
		id = null;
		dir = null;
		dist = 0;
	}
	
	public String toString()
	{
		if (this == null)
		{
			return "null";
		}
		
		return id + " " + dir + " " + dist;
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
	public static TailInfoAvro find_tail(
	    Map<String, GraphNode> nodes, GraphNode startnode, String startdir) 
	        throws IOException {
		//System.err.println("find_tail: " + startnode.getNodeId() + " " + startdir);
		Set<String> seen = new HashSet<String>();
		seen.add(startnode.getNodeId());
		
		GraphNode curnode = startnode;
		CharSequence curdir = startdir;
		String curid = startnode.getNodeId();
		int dist = 0;
		
		boolean canCompress = false;
		
		do
		{
			canCompress = false;			
			TailInfoAvro next = curnode.gettail(curdir);
			
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
				TailInfoAvro nexttail = curnode.gettail(
				    DNAUtil.flip_dir(next.dir.toString()));
				
				if ((nexttail != null) && (nexttail.id.equals(curid)))
				{
					dist++;
					canCompress = true;					
					curid = next.id.toString();
					curdir = next.dir;
				}
			}
		}
		while (canCompress);

		TailInfoAvro retval = new TailInfoAvro();
		
		retval.id = curid;
		retval.dir = DNAUtil.flip_dir(curdir.toString());
		retval.dist = dist;
			
		return retval;
	}
}
