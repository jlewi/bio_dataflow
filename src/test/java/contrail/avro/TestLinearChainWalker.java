package contrail.avro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.Test;

import contrail.DestForLinkDir;
import contrail.EdgeDestNode;
import contrail.GraphNodeData;
import contrail.sequences.DNADirection;

public class TestLinearChainWalker {

	/**
	 * Contains information about a chain constructed for some test.
	 */
	private static class ChainNode {
		// GraphNodes in the order they are connected.
		GraphNode graph_node;		
		
		// The direction for the dna in this chain.
		DNADirection dna_direction;
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
			chain_node.dna_direction = DNADirection.random();
			chain.add(chain_node);
		}
		
		// Now connect the nodes.
		for (int pos = 0; pos < length; pos++) {
			// Add the outgoing edge.
			if (pos + 1 < length) {
				ChainNode src = chain.get(pos);
				ChainNode dest = chain.get(pos + 1);
				GraphNodeData node_data = chain.get(pos).graph_node.getData();
				EdgeDestNode dest_node = new EdgeDestNode();
				dest_node.setNodeId(chain.get(pos + 1).graph_node.getNodeId());
								
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
				dest_node.setNodeId(src.graph_node.getNodeId());
								
				node_data.getDestNodes().add(dest_node);
				
				DestForLinkDir dest_for_link_dir = new DestForLinkDir();
				
				// We need to flip the dna direction to get incoming 
				// edges. 
				dest_for_link_dir.setLinkDir(
						src.dna_direction.flip().toString() +
						dest.dna_direction.flip().toString());
				
				
			}
		}
	}
	@Test
	public void testLinearChainWalker () {
		
	}
}
