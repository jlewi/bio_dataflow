package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;

import contrail.DestForLinkDir;
import contrail.EdgeDestNode;
import contrail.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;

public class TestLinearChainWalker {
	
	// Random number generator.
	private Random generator;
	@Before 
	public void setUp() {
		// Create a random generator so we can make a test repeatable
		// generator = new Random(103);
		generator = new Random();
	}
	/**
	 * Contains information about a chain constructed for some test.
	 */
	private static class ChainNode {
		// GraphNodes in the order they are connected.
		GraphNode graph_node;		
		
		// The direction for the dna in this chain.
		DNAStrand dna_direction;
		
		public String toString() {
			return graph_node.getNodeId() + ":" + dna_direction.toString();
		}
	}
	/**
	 * 
	 * @param length
	 * @return
	 */
	private ArrayList<ChainNode> ConstructChain(int length) {
		ArrayList<ChainNode> chain = new ArrayList<ChainNode>();

		// Construct the nodes.
		for (int pos = 0; pos < length; pos++) {
			// Construct a graph node.
			GraphNode node = new GraphNode();
			GraphNodeData node_data = new GraphNodeData();
			node.setData(node_data);
			node_data.setNodeId("node_" + pos);
			node_data.setDestNodes(new ArrayList<EdgeDestNode>());
			
			ChainNode chain_node = new ChainNode();
			chain_node.graph_node = node;
			chain_node.dna_direction = DNAStrand.random(generator);
			chain.add(chain_node);
		}
		
		// Now connect the nodes.
		// TODO(jlewi): We should add incoming and outgoing edges to the first
		// and last node so that we test that walker stops when the node isn't
		// in memory.
		for (int pos = 0; pos < length; pos++) {
			// Add the outgoing edge.
			if (pos + 1 < length) {
				ChainNode src = chain.get(pos);
				ChainNode dest = chain.get(pos + 1);
				GraphNodeData node_data = src.graph_node.getData();
				EdgeDestNode dest_node = new EdgeDestNode();
				dest_node.setNodeId(dest.graph_node.getNodeId());
								
				node_data.getDestNodes().add(dest_node);
				
				DestForLinkDir dest_for_link_dir = new DestForLinkDir();
				dest_for_link_dir.setLinkDir(
						src.dna_direction.toString() +
						dest.dna_direction.toString());
				dest_node.setLinkDirs(new ArrayList<DestForLinkDir> ());
				dest_node.getLinkDirs().add(dest_for_link_dir);
			}
			
			// Add the incoming edge.
			if (pos > 0) {
				ChainNode src = chain.get(pos);
				ChainNode dest = chain.get(pos - 1);
				GraphNodeData node_data =src.graph_node.getData();
				EdgeDestNode dest_node = new EdgeDestNode();
				dest_node.setNodeId(dest.graph_node.getNodeId());
								
				node_data.getDestNodes().add(dest_node);
				
				DestForLinkDir dest_for_link_dir = new DestForLinkDir();
				
				// We need to flip the dna direction to get incoming 
				// edges. 
				StrandsForEdge linkdir = 
						StrandsForEdge.form(src.dna_direction.flip(), 
											dest.dna_direction.flip());
				dest_for_link_dir.setLinkDir(linkdir.toString());
				dest_node.setLinkDirs(new ArrayList<DestForLinkDir> ());
				dest_node.getLinkDirs().add(dest_for_link_dir);
				
			}
		}
		return chain;
	}
	
	/*
	 * A trial consists of iterating over the chain starting at start_pos on
	 * strand start_strand in direction walk_direction. We use the actual
	 * chain to evaluate whether the walk is correct.
	 */
	public void runTrial(ArrayList<ChainNode> chain,
						 Map<String, GraphNode> nodes_in_memory,
						 int start_pos,  
			             EdgeDirection walk_direction) {
		
		ChainNode chain_start = chain.get(start_pos);
		GraphNode start_node = chain_start.graph_node;
		DNAStrand start_strand = chain_start.dna_direction;
		
		// Construct the iterator
		LinearChainWalker walker = new LinearChainWalker(
				nodes_in_memory, start_node, start_strand,
				walk_direction);
		
		
		int end_pos = -1;
		// Compute what the last node in the chain should be. We
		// need to consider both start_strand and walk direction to figure
		// out which end the chain ends on.
		if (walk_direction == EdgeDirection.OUTGOING) {
			end_pos = chain.size() -1;
		} else {
			end_pos = 0;
		}

		
		int pos_increment = end_pos >= start_pos ? 1 : -1; 
		
		int pos = start_pos;
		while (walker.hasNext()) {
			EdgeTerminal node = walker.next();			
			pos = pos + pos_increment;
			
			// Check the node equals the correct node.
			assertEquals(node.nodeId, 
						 chain.get(pos).graph_node.getNodeId());			
			assertEquals(node.strand, chain.get(pos).dna_direction);
		}
	}
	
	public Map<String, GraphNode> getNodeMap(ArrayList<ChainNode> chain) {
		HashMap<String, GraphNode> map = new HashMap<String, GraphNode>();
		
		for (Iterator<ChainNode> it = chain.iterator(); it.hasNext();) {
			ChainNode chain_node = it.next();
			GraphNode node = chain_node.graph_node;
			map.put(node.getNodeId(), node);
		}
		return map;
	}
	
	@Test
	public void testLinearChainWalker () {		
		int chain_length = generator.nextInt(100) + 5;
		ArrayList<ChainNode> chain = ConstructChain(chain_length);
		Map<String, GraphNode> nodes_map = getNodeMap(chain);
		// How many trials to run. Each trial starts from a different
		// position and strand.
		int num_trials = 10;
		for (int trial = 0; trial < num_trials; trial++) {
			// Which node and strand to start on.
			int start_pos = generator.nextInt(chain_length);
			EdgeDirection walk_direction = EdgeDirection.random(generator);
			runTrial(chain, nodes_map, start_pos, walk_direction);
		}
	}
}
