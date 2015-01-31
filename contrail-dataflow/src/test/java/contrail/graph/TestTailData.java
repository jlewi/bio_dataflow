package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import org.junit.Before;
import org.junit.Test;

import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
public class TestTailData {

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
			node_data.setNeighbors(new ArrayList<NeighborData>());

			ChainNode chain_node = new ChainNode();
			chain_node.graph_node = node;
			chain_node.dna_direction = DNAStrandUtil.random(generator);
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
				NeighborData dest_node = new NeighborData();
				dest_node.setNodeId(dest.graph_node.getNodeId());

				node_data.getNeighbors().add(dest_node);

				EdgeData edge_data = new EdgeData();
				edge_data.setReadTags(new ArrayList<CharSequence>());
				edge_data.setStrands(StrandsUtil.form(
						src.dna_direction, dest.dna_direction));
				dest_node.setEdges(new ArrayList<EdgeData> ());
				dest_node.getEdges().add(edge_data);
			}

			// Add the incoming edge.
			if (pos > 0) {
				ChainNode src = chain.get(pos);
				ChainNode dest = chain.get(pos - 1);
				GraphNodeData node_data =src.graph_node.getData();
				NeighborData dest_node = new NeighborData();
				dest_node.setNodeId(dest.graph_node.getNodeId());

				node_data.getNeighbors().add(dest_node);

				EdgeData edge_data = new EdgeData();
				edge_data.setReadTags(new ArrayList<CharSequence>());
				// We need to flip the dna direction to get incoming
				// edges.
				StrandsForEdge strands =
						StrandsUtil.form(DNAStrandUtil.flip(src.dna_direction),
						    DNAStrandUtil.flip(dest.dna_direction));
				edge_data.setStrands(strands);
				dest_node.setEdges(new ArrayList<EdgeData> ());
				dest_node.getEdges().add(edge_data);

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

		// Find the tail.
		TailData tail = TailData.findTail(
				nodes_in_memory, start_node, start_strand, walk_direction);

		if (start_pos == end_pos) {
			assertEquals(tail, null);
		} else {
			// Check the node equals the correct node.
			assertEquals(tail.terminal.nodeId,
						 chain.get(end_pos).graph_node.getNodeId());
			assertEquals(
					tail.terminal.strand, chain.get(end_pos).dna_direction);
			assertEquals(tail.dist, Math.abs(start_pos - end_pos));
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
	public void testLinearTail () {
		int chain_length = generator.nextInt(100) + 5;
		ArrayList<ChainNode> chain = ConstructChain(chain_length);
		Map<String, GraphNode> nodes_map = getNodeMap(chain);
		// How many trials to run. Each trial starts from a different
		// position and strand.
		int num_trials = 10;
		for (int trial = 0; trial < num_trials; trial++) {
			// Which node and strand to start on.
			int start_pos = generator.nextInt(chain_length);
			EdgeDirection walk_direction = EdgeDirectionUtil.random(generator);
			runTrial(chain, nodes_map, start_pos, walk_direction);
		}
	}

	@Test
	public void testBranch () {
		// We construct the following graph.
		// A->B->C  E->C.
		// We check that findTail starting at A stops at B.
		int chain_length = 3;
		ArrayList<ChainNode> chain = ConstructChain(chain_length);
		Map<String, GraphNode> nodes_map = getNodeMap(chain);

		// Add the branch edge.
		{
			GraphNode branch_node = new GraphNode();
			GraphNodeData node_data = new GraphNodeData();
			DNAStrand branch_strand = DNAStrand.FORWARD;
			branch_node.setData(node_data);
			node_data.setNodeId("branch");

			EdgeTerminal branch_terminal = new EdgeTerminal(
					branch_node.getNodeId(), branch_strand);

			nodes_map.put(branch_node.getNodeId(), branch_node);

			GraphNode last_node = chain.get(chain_length -1).graph_node;
			DNAStrand last_strand = chain.get(chain_length -1).dna_direction;
			last_node.addIncomingEdge(last_strand, branch_terminal);
		}

		{
			// Find the tail starting at A.
			GraphNode start_node = chain.get(0).graph_node;
			DNAStrand start_strand = chain.get(0).dna_direction;
			TailData tail = TailData.findTail(
					nodes_map, start_node, start_strand,
					EdgeDirection.OUTGOING);

			// Check the node equals the correct node.
			int end_pos = chain_length - 2;
			assertEquals(tail.terminal.nodeId,
						 chain.get(end_pos).graph_node.getNodeId());
			assertEquals(
					tail.terminal.strand, chain.get(end_pos).dna_direction);
			assertEquals(tail.dist, chain_length - 2);
		}

		{
			// Find the tail starting at C. No tail should be found because
			// C has two incoming edges.
			GraphNode start_node = chain.get(chain_length - 1).graph_node;
			DNAStrand start_strand = chain.get(1).dna_direction;
			TailData tail = TailData.findTail(
					nodes_map, start_node, start_strand,
					EdgeDirection.OUTGOING);

			assertEquals(tail, null);
		}
	}

	@Test
	public void testCycle() {
	  // A special test case to ensure cycles are properly detected.
    final int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString("ATTCATT", K);

    // The KMers where we should start the search.
    // We start at all KMers in the cycle to make sure te start doesn't matter.
    String[] start_kmers = {"ATT", "TTC", "TCA", "CAT"};
    for (String start: start_kmers) {
      EdgeTerminal terminal = graph.findEdgeTerminalForSequence(start);
      GraphNode start_node = graph.getNode(terminal.nodeId);

      TailData tail = TailData.findTail(
          graph.getAllNodes(), start_node, DNAStrandUtil.random(generator),
          EdgeDirectionUtil.random(generator));

      assertTrue(tail.hit_cycle);
      assertEquals(
          graph.getAllNodes().keySet(), tail.nodes_in_tail);
    }
	}
}
