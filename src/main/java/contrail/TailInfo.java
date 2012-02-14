package contrail;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to represent a tail. We get a tail when 
 * a node has out degree 1. 
 * 
 */
public class TailInfo 
{
	/**
	 * id identifies the second node in the edge. 
	 * 
	 * Node.gettail sets the id to the compressed k-mer representation of the
	 * destination node for the edge.
	 */
	public String id;
	/**
	 * dir is the direction in which the destination node k-mer is oriented. Can be either
	 * "r" or "f".
	 */
	public String dir;
	/**
	 * dist is the number of edges this tail spans. 
	 */
	public int    dist;
	
	/**
	 * Copy Constructor.
	 * 
	 * @param o
	 */
	public TailInfo(TailInfo o)
	{
		id   = o.id;
		dir  = o.dir;
		dist = o.dist;
	}
	
	public TailInfo()
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
	 * Check if the startdir is part of a tail; i.e a sequence of nodes which have outdegree 1.
	 * If it is then we can compress the chain into a longer sequence because there's only one
	 * path through the nodes.
	 * 
	 * @param nodes - A Mapping of strings to nodes. 
	 * @param startnode - Node where we begin our search for the tail. 
	 * @param startdir - The direction to consider for the chain.
	 * @return An instance of TailInfo describing the tail/chain. The id of the tail will be the kmer sequence
	 *         of the last node in the chain. dist will be the number of edges spanned.
	 * @throws IOException
	 */
	public static TailInfo find_tail(Map<String, Node> nodes, Node startnode, String startdir) throws IOException
	{
		//System.err.println("find_tail: " + startnode.getNodeId() + " " + startdir);
		Set<String> seen = new HashSet<String>();
		seen.add(startnode.getNodeId());
		
		Node curnode = startnode;
		String curdir = startdir;
		String curid = startnode.getNodeId();
		int dist = 0;
		
		boolean canCompress = false;
		
		do
		{
			canCompress = false;
			
			TailInfo next = curnode.gettail(curdir);
			
			//System.err.println(curnode.getNodeId() + " " + curdir + ": " + next);

			if ((next != null) &&
				(nodes.containsKey(next.id)) &&
				(!seen.contains(next.id)))
			{
				// curnode is a tail (has outgoing degree 1) and the destination node for this
				// edge is in nodes and we haven't seen it before
				
				seen.add(next.id);
				curnode = nodes.get(next.id);

				// look at the next node and flip its direction.
				// We now check to see whether the next node has a tail 
				// which terminates on the curr node. If it does
				// then there is only a single path between the nodes (i.e they
				// form a chain with no branching and we can compress the nodes together)
				TailInfo nexttail = curnode.gettail(Node.flip_dir(next.dir));
				
				if ((nexttail != null) && (nexttail.id.equals(curid)))
				{
					dist++;
					canCompress = true;
					
					curid = next.id;
					curdir = next.dir;
				}
			}
		}
		while (canCompress);

		TailInfo retval = new TailInfo();
		
		retval.id = curid;
		retval.dir = Node.flip_dir(curdir);
		retval.dist = dist;
			
		return retval;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
