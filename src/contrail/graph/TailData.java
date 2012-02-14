package contrail.graph;

import contrail.avro.GraphNode;
import contrail.avro.NotImplementedException;
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
	public static TailData find_tail(
	    Map<String, GraphNode> nodes, GraphNode startnode, 
	    DNAStrand start_strand, EdgeDirection direction) throws IOException {		
		Set<String> seen = new HashSet<String>();
		seen.add(startnode.getNodeId());
		
		GraphNode curnode = startnode;		
		String curid = startnode.getNodeId();
		int dist = 0;
		
		boolean canCompress = false;
		
		LinearChainWalker walker = new LinearChainWalker(
				nodes, startnode, start_strand, direction);
		
		while(walker.hasNext()) {
			// The walker only returnes a terminal if the corresponding
			// GraphNode is in the map passed to LinearChainWalker.
			EdgeTerminal terminal = walker.next();
			
			if (!seen.contains(terminal.nodeId)) {
				// curnode has a tail (has outgoing degree 1); the tail 
//				// is in nodes and we haven't seen it before				
				seen.add(terminal.nodeId);
//				curnode = nodes.get(next.id);
//
//				// We can only compress the tail if the tail has a single incoming edge.
//				// To check whether curnode has a single incoming edge, we look
//				// for outgoing edges with the direction for the source flipped; this
//				// produces a list of incoming edges. If there is a single incoming
//				// edge then there is only a single path between the nodes (i.e they
//				// form a chain with no branching and we can compress the nodes together)
//				TailData nexttail = curnode.getTail(next.strand.flip());
//				
//				if ((nexttail != null) && (nexttail.id.equals(curid)))
//				{
//					dist++;
//					canCompress = true;					
//					curid = next.id.toString();
//					curdir = next.strand;
//				}
			}
			throw new RuntimeException("Left of here");			
		}
		throw new NotImplementedException("Need to finish code");
		
//		do
//		{
//			canCompress = false;			
//			TailData next = curnode.getTail(start_strand, direction);
//			
//			if ((next != null) &&
//				(nodes.containsKey(next.terminal.nodeId)) &&
//				(!seen.contains(next.terminal.nodeId)))
//			{
//				// curnode has a tail (has outgoing degree 1); the tail 
//				// is in nodes and we haven't seen it before				
//				seen.add(next.id.toString());
//				curnode = nodes.get(next.id);
//
//				// We can only compress the tail if the tail has a single incoming edge.
//				// To check whether curnode has a single incoming edge, we look
//				// for outgoing edges with the direction for the source flipped; this
//				// produces a list of incoming edges. If there is a single incoming
//				// edge then there is only a single path between the nodes (i.e they
//				// form a chain with no branching and we can compress the nodes together)
//				TailData nexttail = curnode.getTail(next.strand.flip());
//				
//				if ((nexttail != null) && (nexttail.id.equals(curid)))
//				{
//					dist++;
//					canCompress = true;					
//					curid = next.id.toString();
//					curdir = next.strand;
//				}
//			}
//		}
//		while (canCompress);
//
//		TailData retval = new TailData();
//		
//		retval.id = curid;
//		// JLEWI: This is an abuse of dir; it shouldn't be used
//		// to represent both the direction of the tail node and the tail 
//		// direction. 
//		retval.strand = curdir.flip();
//		retval.dist = dist;
			
		//return retval;
	}
	
}
