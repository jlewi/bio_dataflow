package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import contrail.DestForLinkDir;
import contrail.EdgeDestNode;
import contrail.GraphNodeData;
import contrail.avro.EdgeDirection;
import contrail.avro.GraphNode;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
//import contrail.sequences.DNAStrand;
import org.junit.Before;
import org.junit.Test;

public class TestGraphNode {

	// Random number generator.
	private Random generator;
	@Before 
	public void setUp() {
		// Create a random generator.
		generator = new Random();
		//generator = new Random(103);
	}
	
	/**
	 * @return A random node to use in the tests.
	 */
	private GraphNode createNode() {
		GraphNode node = new GraphNode();
		GraphNodeData node_data = new GraphNodeData();
		node.setData(node_data);
		node_data.setNodeId("node");
		node_data.setDestNodes(new ArrayList<EdgeDestNode>());
		
		int num_dest_nodes = generator.nextInt(30) + 5;
		//int num_dest_nodes = 2;
		for (int index = 0; index < num_dest_nodes; index++) {
			EdgeDestNode dest_node = new EdgeDestNode();
			dest_node.setNodeId("dest_" + index);
			node_data.getDestNodes().add(dest_node);
			dest_node.setLinkDirs(new ArrayList<DestForLinkDir> ());
			
			// generate some links for this node.
			int num_links = 
					generator.nextInt(StrandsForEdge.values().length) + 1;
			
			for (int link_index = 0; link_index < num_links; link_index++) {
				DestForLinkDir dest_for_linkdir = new DestForLinkDir();
				dest_for_linkdir.setLinkDir(
						StrandsForEdge.values()[link_index].toString());
				dest_node.getLinkDirs().add(dest_for_linkdir);
			}			
		}
		return node;
	}
	
	/**
	 * Return a list of all the edge terminals for the specified strand
	 * of the node.
	 * @param node
	 * @param strand
	 * @return
	 */
	private List<EdgeTerminal> getAllEdgeTerminalsForStrand(
			GraphNode node, DNAStrand strand) {
		List<EdgeTerminal> all_terminals = new ArrayList<EdgeTerminal>();
		
		GraphNodeData node_data = node.getData();
		for (Iterator<EdgeDestNode> edge_it = 
				node_data.getDestNodes().iterator(); edge_it.hasNext();) {
			EdgeDestNode dest_node = edge_it.next();
			for (Iterator<DestForLinkDir> link_it = 
					dest_node.getLinkDirs().iterator(); link_it.hasNext();) {								
				DestForLinkDir dest_for_link_dir = link_it.next();
				StrandsForEdge edge_strands = 
						StrandsForEdge.parse(
								dest_for_link_dir.getLinkDir().toString());
				if (edge_strands.src() != strand) {
					continue;
				}
				all_terminals.add(
						new EdgeTerminal(dest_node.getNodeId().toString(), 
										 edge_strands.dest()));
			}
		}
		return all_terminals;
	}
	@Test
	public void testLists () {
		GraphNode node = createNode();
		
		// This list will be all the edge terminals stored in
		// the node. We will use this list to check all nodes in the derived
		// lists are actually in the node.
		List<EdgeTerminal> all_f_edge_terminals = 
				getAllEdgeTerminalsForStrand(node, DNAStrand.FORWARD);
		List<EdgeTerminal> all_r_edge_terminals = 
				getAllEdgeTerminalsForStrand(node, DNAStrand.REVERSE);
		
		
		// Check the requisite lists.
		List<EdgeTerminal> f_outgoing = node.getEdgeTerminals(
				DNAStrand.FORWARD, EdgeDirection.OUTGOING);
		List<EdgeTerminal> f_incoming = node.getEdgeTerminals(
				DNAStrand.FORWARD, EdgeDirection.INCOMING);
		List<EdgeTerminal> r_outgoing = node.getEdgeTerminals(
				DNAStrand.REVERSE, EdgeDirection.OUTGOING);
		List<EdgeTerminal> r_incoming = node.getEdgeTerminals(
				DNAStrand.REVERSE, EdgeDirection.INCOMING);
		
		// Make sure all the edge terminals are actually in the node.
		assertTrue(all_f_edge_terminals.containsAll(f_outgoing));
		assertTrue(all_r_edge_terminals.containsAll(f_incoming));
		
		assertTrue(all_r_edge_terminals.containsAll(r_outgoing));
		assertTrue(all_f_edge_terminals.containsAll(r_incoming));
		
		// Check the reverse
		{
			List<EdgeTerminal> f_edges = new ArrayList<EdgeTerminal>();
			f_edges.addAll(f_outgoing);
			f_edges.addAll(f_incoming);
			assertTrue(f_edges.containsAll(all_f_edge_terminals));
		}
		
		{
			List<EdgeTerminal> r_edges = new ArrayList<EdgeTerminal>();
			r_edges.addAll(r_outgoing);
			r_edges.addAll(r_incoming);
			assertTrue(r_edges.containsAll(all_r_edge_terminals));
		}
	}
}
