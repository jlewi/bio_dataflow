package contrail.graph;

import contrail.CompressedSequence;
import contrail.EdgeData;
import contrail.NeighborData;
import contrail.GraphNodeData;
import contrail.GraphNodeKMerTag;
import contrail.R5Tag;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.KMerReadTag;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.TailData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Wrapper class for accessing modifying the GraphNodeData structure.
 * 
 * This class is used to represent the output of BuildGraphAvro.
 * The class represents a node (KMer) in the graph and all outgoing
 * edges from that sequence
 */
public class GraphNode {

	private GraphNodeData data;

	/**
	 * Class is a container for all the derived data for the node that
	 * we want to avoid recomputing each time it is accessed.
	 */
	protected static class DerivedData {
		private GraphNodeData data;


		// Whether we have created the edge lists.
		private boolean lists_created;

		// Store list of edge terminals for the specified strand in the given 
		// direction these lists are immutable so that we can safely return 
		// references to the caller.

		// For each strand we store a list of outgoing and incoming edges.
		private List<EdgeTerminal> f_outgoing_edges;
		private List<EdgeTerminal> r_outgoing_edges;
		private List<EdgeTerminal> f_incoming_edges;
		private List<EdgeTerminal> r_incoming_edges;

		public DerivedData(GraphNodeData data) {
			this.data = data;
			lists_created = false;
		}
		/**
		 * This hash map maps the enum StrandsForEdge to a list of strings  
		 * which are the node ids for the neighbors.
		 * If there are no ids for this value of StrandsForEdge the list is 
		 * empty (not null);
		 */
		private HashMap<StrandsForEdge, List<CharSequence>> 
		strands_to_neighbors;


		// TODO(jlewi): Would it be better to use sorted sets for fast lookups?
		private void createEdgeLists() {
			lists_created = true;

			strands_to_neighbors = 
					new HashMap<StrandsForEdge, List<CharSequence>> ();
			// Initialize the lists.
			strands_to_neighbors.put(
					StrandsForEdge.FF, new ArrayList<CharSequence> ());
			strands_to_neighbors.put(
					StrandsForEdge.FR, new ArrayList<CharSequence> ());
			strands_to_neighbors.put(
					StrandsForEdge.RF, new ArrayList<CharSequence> ());
			strands_to_neighbors.put(
					StrandsForEdge.RR, new ArrayList<CharSequence> ());

			f_outgoing_edges = new ArrayList<EdgeTerminal>();
			r_outgoing_edges = new ArrayList<EdgeTerminal>();

			NeighborData dest_node;
			List<CharSequence> id_list;

			// Loop over the destination nodes.
			for (Iterator<NeighborData> it = data.getNeighbors().iterator();
					it.hasNext();) {
				dest_node = it.next();
				List<EdgeData> list_link_dirs = dest_node.getEdges();
				if (list_link_dirs == null) {
					throw new RuntimeException(
							"The list of StrandsForEdge is null for destination:" +
					        dest_node.getNodeId() + 
					        " this should not happen.");
				}
				for (Iterator<EdgeData> edge_it = 
						dest_node.getEdges().iterator();
				     edge_it.hasNext();) {
					EdgeData edge_data = edge_it.next();
					StrandsForEdge strands = edge_data.getStrands();
					id_list = strands_to_neighbors.get(strands);
					id_list.add(dest_node.getNodeId());

					if (StrandsUtil.src(strands) == DNAStrand.FORWARD) {
						f_outgoing_edges.add(
								new EdgeTerminal(dest_node.getNodeId().toString(), 
										StrandsUtil.dest(strands)));
					} else {
						r_outgoing_edges.add(
								new EdgeTerminal(dest_node.getNodeId().toString(), 
										StrandsUtil.dest(strands)));
					}
				}
			}

			// We can construct the incoming edges as follows,
			// An edge x->y implies an edge RC(y)->RC(X) where RC is the reverse
			// complement. Thus the edge  w->x implies an edge RC(X) -> RC(W)
			// So to get the incoming edges for x we look at outgoing edges
			// RC(x) and then flip the direction of the destination.
			f_incoming_edges = new ArrayList<EdgeTerminal>();
			for (Iterator<EdgeTerminal> it = r_outgoing_edges.iterator();
					it.hasNext();) {
				EdgeTerminal terminal = it.next();
				f_incoming_edges.add(new EdgeTerminal(
						terminal.nodeId, terminal.strand.flip()));
			}

			r_incoming_edges = new ArrayList<EdgeTerminal>();
			for (Iterator<EdgeTerminal> it = f_outgoing_edges.iterator();
					it.hasNext();) {
				EdgeTerminal terminal = it.next();
				r_incoming_edges.add(new EdgeTerminal(
						terminal.nodeId, terminal.strand.flip()));
			}

			// Convert the lists to immutable lists.
			f_outgoing_edges = Collections.unmodifiableList(f_outgoing_edges);
			r_outgoing_edges = Collections.unmodifiableList(r_outgoing_edges);
			f_incoming_edges = Collections.unmodifiableList(f_incoming_edges);
			r_incoming_edges = Collections.unmodifiableList(r_incoming_edges);

			for (StrandsForEdge strands : StrandsForEdge.values()) {
				id_list = Collections.unmodifiableList(
						strands_to_neighbors.get(strands));
				strands_to_neighbors.put(strands, id_list);
			}

		}
		public List<CharSequence> getNeighborsForStrands(StrandsForEdge strands) {
			if (!lists_created) {
				createEdgeLists();
			}
			return strands_to_neighbors.get(strands);
		}

		/**
		 * Retuns an immutable list of the terminals for outgoing or incoming edges.
		 * @param strand: Which strand in this node to consider.
		 * @param direction: Direction of the edge to consider.
		 */
		public List<EdgeTerminal>  getEdgeTerminals(
				DNAStrand strand, EdgeDirection direction) {
			if (!lists_created) {
				createEdgeLists();
			}
			List<EdgeTerminal> terminals;
			if (strand == DNAStrand.FORWARD) {
				if (direction == EdgeDirection.OUTGOING) {    			
					terminals = f_outgoing_edges;
				} else {
					terminals = f_incoming_edges;
				}    		
			} else {
				if (direction == EdgeDirection.OUTGOING) {    			
					terminals = r_outgoing_edges;
				} else {
					terminals = r_incoming_edges;
				}
			}
			return terminals;
		}
		
		/**
		 * Clear any precomputed data. This function needs to be called
		 * whenever the graph changes so that we don't return stale data;
		 */
		public void clear() {
			lists_created = false;
			f_outgoing_edges = null;
			r_outgoing_edges = null;
			f_incoming_edges = null;
			r_incoming_edges = null;
			strands_to_neighbors = null;
		}
	}

	protected DerivedData derived_data;

	/**
	 * Construct a new object with a new GraphNodeData to store the data.
	 */
	public GraphNode() {
		data = new GraphNodeData();   
		// Initialize any member variables so that if we serialzie it we don't 
		// get null objects.
		data.setR5Tags(new ArrayList<R5Tag>());
	}

	/**
	 * Add information about a destination KMer which came from the start of a read.
	 * 
	 * If the destination KMer came from the start of a read, then the edge
	 * Source KMer -> Destination KMer allows us to connect two reads. 
	 * 
	 * @param tag - String identifying the read from where the destination KMer came
	 *              from. 
	 * @param offset - 0 if isRC is false, K-1 otherwise.
	 * @param isRC - True if the actual KMer is the reverse complement of the 
	 *     canonical version of the KMer.
	 * @param maxR5 - Maximum number of r5 tags to store.
	 */
	public void addR5(KMerReadTag tag, int offset, boolean isRC, int maxR5)
	{

		if (data.r5_tags.size() < maxR5)
		{
			R5Tag val = new R5Tag();

			val.tag = tag.toString();
			val.offset = offset;
			val.isRC = val.isRC;
			data.r5_tags.add(val);
		}
	}

	public void setMertag(KMerReadTag mertag) {
		GraphNodeKMerTag tag = new GraphNodeKMerTag(); 
		tag.setReadTag(mertag.read_id);
		tag.setChunk(mertag.chunk);
		data.setMertag(tag);
	}

	public void setCoverage(int cov) {
		data.coverage = cov;
	}

	/** 
	 * @param seq
	 */
	public void setCanonicalSourceKMer(Sequence seq) {
		CompressedSequence compressed = new CompressedSequence();
		compressed.setDna(ByteBuffer.wrap(seq.toPackedBytes(), 0, seq.numPackedBytes()));
		compressed.setLength(seq.size()); 
		data.setCanonicalSourceKmer(compressed);  
	}

	/** 
	 * @param seq
	 */
	public void setCanonicalSourceKMer(ByteBuffer seq, int length) {
		CompressedSequence compressed = new CompressedSequence();
		compressed.setDna(seq);
		compressed.setLength(length); 
		data.setCanonicalSourceKmer(compressed);  
	}

	/**
	 * Return a reference to the GraphNodeData object storing all the data.
	 * @return
	 */
	public GraphNodeData getData() {
		return data;
	}

	/**
	 * Add an outgoing edge to this node.
	 *
	 * @param link_dir - Two letter string such as 'ff' which indicates the canonical
	 *   direction of the source and destination KMers. 
	 * @param canonical_dest - The canonical sequence for the destination KMer.
	 * @param tag - A string identifying the read (and location within the read) where this 
	 *   edge came from.
	 * @param MAXTHREADREADS - Maximum number of threads to record the tags for.
	 */
//	public void addEdge(CharSequence link_dir, Sequence canonical_dest, String tag, long MAXTHREADREADS) {
//		if (data.getNeighbors() == null) {
//			data.setDestNodes(new ArrayList<NeighborData>());
//		}
//		// Get a list of the edges for this direction.
//		// Create the list if it doesn't exist.
//		NeighborData dest_node = findDestNode(canonical_dest);
//
//		if (dest_node == null) {
//			dest_node = new NeighborData();
//			CompressedSequence compressed = new CompressedSequence();
//			compressed.setDna(ByteBuffer.wrap(canonical_dest.toPackedBytes(), 0, canonical_dest.numPackedBytes()));
//			compressed.setLength(canonical_dest.size());      
//			dest_node.setCanonicalSequence(compressed);
//			dest_node.setLinkDirs(new ArrayList<EdgeData>());
//			data.getNeighbors().add(dest_node);
//		}
//
//
//		// Check if we already have an instance for this link direction.
//		EdgeData edge_info = findInstancesForLinkDir(dest_node, link_dir);
//
//		if (edge_info == null) {
//			edge_info = new EdgeData();
//			edge_info.setReadTags(new ArrayList<CharSequence>());
//			dest_node.getEdges().add(edge_info);      
//		}
//
//		edge_info.setLinkDir(link_dir);
//
//		if (edge_info.getReadTags().size() < MAXTHREADREADS)
//		{
//			edge_info.getReadTags().add(tag);
//		}
//	}

	/**
	 * Find the instance of NeighborData representing the given canonical_sequence.
	 * @param canonical_sequence
	 * @return The instance if found or null otherwise.
	 * 
	 * TODO(jlewi): We could probably speed this up by creating a hash map for the 
	 * destination sequences.
	 */
//	private NeighborData findDestNode(Sequence canonical_sequence) {
//		// TODO(jlewi): We should probably get rid of this function
//		// and just use findDestNode(String nodeId)
//		if (data.getNeighbors() == null) {
//			return null;
//		}
//		Sequence canonical = new Sequence(DNAAlphabetFactory.create());
//		for (Iterator<NeighborData> it_dest_node = data.getNeighbors().iterator();
//				it_dest_node.hasNext();) {
//			NeighborData dest = it_dest_node.next();
//			canonical.readPackedBytes(dest.getCanonicalSequence().getDna().array(), 
//					dest.getCanonicalSequence().getLength());
//
//			if (canonical.equals(canonical_sequence)) {
//				return dest;
//			}
//		}
//		return null;   
//	}

	/**
	 * Find the instance of NeighborData for the specified nodeId.
	 * @param: nodeId 
	 * @return The instance if found or null otherwise.
	 * 
	 * TODO(jlewi): We could probably speed this up by creating a hash map for  
	 * the destination nodeIds. This function should probably go in
	 * derived_data.
	 */
	private NeighborData findNeighbor(String nodeId) {
		if (data.getNeighbors() == null) {
			return null;
		}		
		for (Iterator<NeighborData> it_dest_node = data.getNeighbors().iterator();
				it_dest_node.hasNext();) {
			NeighborData dest = it_dest_node.next();		
			if (dest.getNodeId().toString().equals(nodeId)) {
				return dest;
			}
		}
		return null;   
	}
	  
	/**
	 * Add an outgoing edge to this node. 
	 * @param strand: Which strand to add the edge to.
	 * @param dest: The destination
	 * @param tag: (Optional): String identifying the read where this edge 
	 *   came from.
	 * @param MAXTHREADREADS: The tag will only be recorded if we have fewer
	 *   than this number of tags associated with this read.
	 */
	public void addOutgoingEdge(DNAStrand strand, EdgeTerminal dest, String tag, 
	                            long MAXTHREADREADS) {
		// Clear the derived data.
		this.derived_data.clear();
		NeighborData dest_node = findNeighbor(dest.nodeId);
		
		if (dest_node == null) {
			dest_node = new NeighborData();
			dest_node.setNodeId(dest.nodeId);
			List<NeighborData> neighbors = data.getNeighbors();
			if (neighbors == null) {
				neighbors = new ArrayList<NeighborData>();
				data.setNeighbors(neighbors);
			}
			neighbors.add(dest_node);			
		}
		
		List<EdgeData> list_edge_strands = dest_node.getEdges();
		if (list_edge_strands == null) {
			list_edge_strands = new ArrayList<EdgeData> ();
			dest_node.setEdges(list_edge_strands);
		}
		
		StrandsForEdge strands = StrandsUtil.form(strand, dest.strand);
		EdgeData edge = findEdgeDataForStrands(
				dest_node, strands);
		
		if (edge == null) {
		  edge = new EdgeData();
      list_edge_strands.add(edge);
      edge.setStrands(strands);
      edge.setReadTags(new ArrayList<CharSequence>());
		}
		
		if (tag !=null) {
      if (edge.getReadTags().size() < MAXTHREADREADS){
          edge.getReadTags().add(tag);
      }
		}
		
	}
	
	/**
	 * Add an outgoing edge to this node. 
	 * @param strand: Which strand to add the edge to.
	 * @param dest: The destination
	 */
	public void addOutgoingEdge(DNAStrand strand, EdgeTerminal dest) {
	  addOutgoingEdge(strand, dest, null, 0);
	}
 
	/**
	 * Add an incoming edge to this node. 
	 * @param strand: Which strand to add the edge to.
	 * @param src: The source terminal
	 */
	public void addIncomingEdge(DNAStrand strand, EdgeTerminal src) {
		// Let this node be X.
		// Then edge Y->X implies edge RC(x)->RC(y).
		// So we add RC(x)->RC(y) to this node.		
		EdgeTerminal dest = new EdgeTerminal(src.nodeId, src.strand.flip());
		addOutgoingEdge(strand.flip(), dest);
	}
	
	/**
	 * Find the edge instances inside NeighborData for the given strands.
	 * 
	 * @param canonical_sequence
	 * @return The instance if found or null otherwise.
	 * 
	 * TODO(jlewi): We could probably speed this up by creating a hash map for the 
	 * destination sequences.
	 */
	private EdgeData findEdgeDataForStrands(NeighborData node, 
	                                        StrandsForEdge strands) {
		if (node.getEdges() == null) {
			return null;
		}

		for (Iterator<EdgeData> it = node.getEdges().iterator();
				it.hasNext();) {
			EdgeData edge_info = it.next();
			if (edge_info.getStrands() == strands) {
				return edge_info;
			}
		}
		return null;
	}

	/**
	 * Set the data manipulated by this node.
	 */
	public void setData(GraphNodeData data) {
		this.data = data;
		// Clear the derived data
		this.derived_data = new DerivedData(data);
	}

	/**
	 * Compute the degree for this node. 
	 * 
	 * The degree is the number of outgoing edges from this node when the kmer 
	 * sequence represented by this node is read in direction dir.
	 * 
	 * @param strand - Which strand to consider.
	 * @return 
	 * TODO(jlewi): Add a unittest.
	 */
	public int degree(DNAStrand strand) {
		int retval = 0;


		StrandsForEdge fd = StrandsUtil.form(strand, DNAStrand.FORWARD);    
		retval += this.derived_data.getNeighborsForStrands(fd).size();


		StrandsForEdge rd = StrandsUtil.form(strand, DNAStrand.REVERSE);
		retval += this.derived_data.getNeighborsForStrands(rd).size();

		return retval;
	}

	public String getNodeId() {
		return data.getNodeId().toString(); 
	}

	/**
	 * Return a list of the canonical compressed sequences for the specific 
	 * link direction;
	 * 
	 * @param link_dir: Two letter string representing the canonical direction
	 *   for the source and destination sequences.
	 * @return: List of the CompressedSequences for the canonical 
	 *   represention for the destination KMer.
	 */
	public List<CompressedSequence> getCanonicalEdgeData(String key) {
		throw new RuntimeException("Need to implement this method");
	}

	/**
	 * Return the tail information for this node. If this node has out degree
	 * 1 then its tail is the node we are connected to.
	 * 
	 * @param strand - Which strand of DNA to consider the tail information for.
	 * @param tail_dir - The direction of the tail. Outgoing means follow
	 *                   the outgoing edges. Incoming means follow the incoming
	 *                   edges.
	 * @return - An instance of Tailinfo. ID is set to the  
	 *  representation of the destination node,
	 *   dist is initialized to 1, and dir is the direction coresponding to the 
	 *   destination node.
	 *   
	 * TODO(jlewi): Add a unittest. 
	 * TODO(jlewi): Clean up the docstring once the code is finalized. 
	 */
	public TailData getTail(DNAStrand dir, EdgeDirection tail_dir)
	{    

		TailData ti = new TailData();
		ti.dist = 1;
		ti.direction = tail_dir;


		List<EdgeTerminal> terminals = getEdgeTerminals(dir, tail_dir);
		if (terminals.size() != 1) {
			// No tail because degree isn't 1.
			return null;
		}
		ti.terminal = terminals.get(0);
		return ti;
	}

	/** 
	 * Return a list of the node ids for neighbors with edges corresponding
	 * to the given strands. 
	 * @param link_dir
	 * @return
	 */
	public List<CharSequence> getNeighborsForStrands(StrandsForEdge strands) {
		return derived_data.getNeighborsForStrands(strands);
	}

	/**
	 * This is mostly intended for displaying in a debugger.
	 */
	public String toString() {
		return data.toString();
	}

	/**
	 * Returns an immutable list of the terminals for outgoing or incoming edges.
	 * @param strand: Which strand in this node to consider.
	 * @param direction: Direction of the edge to consider.
	 */
	public List<EdgeTerminal>  getEdgeTerminals(
			DNAStrand strand, EdgeDirection direction) {
		return derived_data.getEdgeTerminals(strand, direction);
	}
}
