package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.sequences.DNAStrandUtil;
import org.junit.Before;
import org.junit.Test;

public class TestGraphNode {

	// Random number generator.
	private Random generator;
	@Before 
	public void setUp() {
		// Create a random generator.
		generator = new Random();
	}

	/**
	 * @return A random node to use in the tests.
	 */
	private GraphNode createNode() {
		GraphNode node = new GraphNode();
		GraphNodeData node_data = new GraphNodeData();
		node.setData(node_data);
		node_data.setNodeId("node");
		node_data.setNeighbors(new ArrayList<NeighborData>());

		int num_dest_nodes = generator.nextInt(30) + 5;
		//int num_dest_nodes = 2;
		for (int index = 0; index < num_dest_nodes; index++) {
			NeighborData dest_node = new NeighborData();
			dest_node.setNodeId("dest_" + index);
			node_data.getNeighbors().add(dest_node);
			dest_node.setEdges(new ArrayList<EdgeData> ());

			// generate some links for this node.
			int num_edges = 
					generator.nextInt(StrandsForEdge.values().length) + 1;

			for (int edge_index = 0; edge_index < num_edges; edge_index++) {
				EdgeData edge = new EdgeData();
				edge.setStrands(StrandsForEdge.values()[edge_index]);
				dest_node.getEdges().add(edge);
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
		
		StrandsForEdge[] strands_array = 
		  {StrandsUtil.form(strand, DNAStrand.FORWARD), 
		   StrandsUtil.form(strand, DNAStrand.REVERSE)};
		    		
		for (StrandsForEdge strands: strands_array) {		 
			List<CharSequence> neighborids = node.getNeighborsForStrands(strands);			
			for (Iterator<CharSequence> nodeid_it = 
					neighborids.iterator(); nodeid_it.hasNext();) {								
				CharSequence nodeid = nodeid_it.next();				
				all_terminals.add(
						new EdgeTerminal(nodeid.toString(), 
								StrandsUtil.dest(strands)));
			}
		}
		return all_terminals;
	}

	private List<EdgeTerminal> flipTerminals(List<EdgeTerminal> terminals) {
		// Flip the edge of each terminal.
		List<EdgeTerminal> flipped = new ArrayList<EdgeTerminal> ();
		for (Iterator<EdgeTerminal> it = terminals.iterator(); it.hasNext();) {
			EdgeTerminal terminal = it.next();
			flipped.add(new EdgeTerminal(
					terminal.nodeId, DNAStrandUtil.flip(terminal.strand)));
		}
		return flipped;
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
		assertTrue(all_r_edge_terminals.containsAll(flipTerminals(f_incoming)));

		assertTrue(all_r_edge_terminals.containsAll(r_outgoing));
		assertTrue(all_f_edge_terminals.containsAll(flipTerminals(r_incoming)));

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

	/**
	 * Return the list of possible link direction in random permutation order
	 * @return
	 */
	protected StrandsForEdge[] permuteEdges() {
		List<StrandsForEdge> link_dirs = new ArrayList<StrandsForEdge>();

		for (StrandsForEdge dir: StrandsForEdge.values()) {
			link_dirs.add(dir);
		}

		StrandsForEdge[] return_type = new StrandsForEdge[]{};
		Collections.shuffle(link_dirs);
		return link_dirs.toArray(return_type);
	}


	public static class CharSequenceComparator implements Comparator {
		public int compare (Object o1, Object o2) {
			return o1.toString().compareTo(o2.toString());
		}
		public boolean equals(Object obj) {
			return obj instanceof CharSequenceComparator;
		}
	}

	@Test
	public void testGetNeighborIdsForDNAStrand() {
		// Create a graph node with some edges and verify getDestIdsForSrcDir
		// returns the correct data.
		GraphNodeData node_data = new GraphNodeData();
		node_data.setNodeId("node");
		node_data.setNeighbors(new ArrayList<NeighborData> ());
		int num_edges = (int) Math.floor(Math.random() * 100) + 1;

		HashMap<StrandsForEdge, List<String>> true_nodes_for_link_dirs =
				new HashMap<StrandsForEdge, List<String>> ();

		for (int index = 0; index < num_edges; index++) {
			NeighborData edge_dest = new NeighborData();
			node_data.getNeighbors().add(edge_dest);
			edge_dest.setNodeId("edge_" + index);

			edge_dest.setEdges(new ArrayList<EdgeData>());

			int num_links = (int) Math.floor(Math.random() * 4) + 1;
			StrandsForEdge[] link_dirs = permuteEdges();
			for (int link_index = 0; link_index < num_links; link_index++) {
				StrandsForEdge dir = link_dirs[link_index];
				EdgeData dest_for_link = new EdgeData();
				dest_for_link.setStrands(dir);
				edge_dest.getEdges().add(dest_for_link);


				// Add this node to true_nodes_for_link_dirs;
				if (!true_nodes_for_link_dirs.containsKey(dir)) {
					true_nodes_for_link_dirs.put(dir, new ArrayList<String>());
				}
				true_nodes_for_link_dirs.get(dir).add(
						edge_dest.getNodeId().toString());
			}
		}

		//********************************************************************
		// Run the test.
		// Create a GraphNode.
		GraphNode node = new GraphNode();
		node.setData(node_data);
		for (StrandsForEdge dir: StrandsForEdge.values()) {
			if (!true_nodes_for_link_dirs.containsKey(dir)) {
				assertEquals(null, node.getNeighborsForStrands(dir));
			} else {
				List<CharSequence> immutable_dest_ids = 
						node.getNeighborsForStrands(dir);
				// Copy the list because the existing list is immutable.
				List<CharSequence> dest_ids = new ArrayList<CharSequence>();
				dest_ids.addAll(immutable_dest_ids);	        
				Collections.sort(dest_ids, new CharSequenceComparator());

				List<String> true_dest_ids = true_nodes_for_link_dirs.get(dir);
				Collections.sort(true_dest_ids);

				assertTrue(true_dest_ids.equals(dest_ids));
			}
		}
	}
	
	@Test
	public void testAddOutgoingEdge() {
		// TODO(jlewi): This test could be a lot more thorough.
		// add multiple edges. For each destination terminal add edge
		// to both strands. 
		
		// Create a graph node with some edges and verify getDestIdsForSrcDir
		// returns the correct data.
		GraphNode node = new GraphNode();
		GraphNodeData node_data = new GraphNodeData();
		node_data.setNodeId("node");
		node.setData(node_data);
		
		GraphNode terminal_node = new GraphNode();
		GraphNodeData terminal_data = new GraphNodeData();		
		terminal_node.setData(terminal_data);
		terminal_data.setNodeId("terminal");
		DNAStrand terminal_strand = DNAStrandUtil.random();
		EdgeTerminal terminal = new EdgeTerminal(
				terminal_node.getNodeId(), terminal_strand);
		
		
		node.addOutgoingEdge(DNAStrand.FORWARD, terminal);
		
		List<EdgeTerminal> outgoing_edges = node.getEdgeTerminals(
				DNAStrand.FORWARD, EdgeDirection.OUTGOING);
		assertEquals(1, outgoing_edges.size());
		assertEquals(terminal, outgoing_edges.get(0));
	}
}
