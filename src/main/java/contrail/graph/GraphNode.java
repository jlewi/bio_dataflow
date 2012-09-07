package contrail.graph;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.TailData;

import contrail.sequences.CompressedSequence;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.KMerReadTag;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.specific.SpecificData;

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
    // TODO(jlewi): We should consider using HashSet's so that lookups
    // for a particular terminal will be fast.
    // Lists, however, provide better guarantees for iterating over the
    // elements.
    private List<EdgeTerminal> f_outgoing_edges;
    private List<EdgeTerminal> r_outgoing_edges;
    private List<EdgeTerminal> f_incoming_edges;
    private List<EdgeTerminal> r_incoming_edges;

    private Set<EdgeTerminal> fOutgoingEdgeSet;
    private Set<EdgeTerminal> rOutgoingEdgeSet;
    private Set<EdgeTerminal> fIncomingEdgeSet;
    private Set<EdgeTerminal> rIncomingEdgeSet;

    // For each strand we store a hash map which maps an EdgeTerminal
    // to a list of the read tags that gave rise to that edge.
    private HashMap<EdgeTerminal, List<CharSequence>> f_edge_tags_map;
    private HashMap<EdgeTerminal, List<CharSequence>> r_edge_tags_map;

    private HashSet<String> neighborIds;

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

      f_edge_tags_map = new HashMap<EdgeTerminal, List<CharSequence>>();
      r_edge_tags_map = new HashMap<EdgeTerminal, List<CharSequence>>();

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

          EdgeTerminal terminal =
              new EdgeTerminal(dest_node.getNodeId().toString(),
                  StrandsUtil.dest(strands));

          // Create an immutable list out of the read tags.
          List<CharSequence> immutable_tags =
              Collections.unmodifiableList(edge_data.getReadTags());
          if (StrandsUtil.src(strands) == DNAStrand.FORWARD) {
            f_outgoing_edges.add(terminal);
            f_edge_tags_map.put(terminal, immutable_tags);
          } else {
            r_outgoing_edges.add(terminal);
            r_edge_tags_map.put(terminal, immutable_tags);
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
            terminal.nodeId, DNAStrandUtil.flip(terminal.strand)));
      }

      r_incoming_edges = new ArrayList<EdgeTerminal>();
      for (Iterator<EdgeTerminal> it = f_outgoing_edges.iterator();
          it.hasNext();) {
        EdgeTerminal terminal = it.next();
        r_incoming_edges.add(new EdgeTerminal(
            terminal.nodeId, DNAStrandUtil.flip(terminal.strand)));
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

      // TODO(jlewi): Should we create a separate function for creating the
      // hash sets so we don't have to create them if all we want is the
      // list versions and vice versa.
      fOutgoingEdgeSet = Collections.unmodifiableSet(f_edge_tags_map.keySet());
      rOutgoingEdgeSet = Collections.unmodifiableSet(r_edge_tags_map.keySet());
      fIncomingEdgeSet = new HashSet<EdgeTerminal>();
      rIncomingEdgeSet = new HashSet<EdgeTerminal>();
      fIncomingEdgeSet.addAll(f_incoming_edges);
      rIncomingEdgeSet.addAll(r_incoming_edges);
      fIncomingEdgeSet = Collections.unmodifiableSet(fIncomingEdgeSet);
      rIncomingEdgeSet = Collections.unmodifiableSet(rIncomingEdgeSet);
    }

    public List<CharSequence> getNeighborsForStrands(StrandsForEdge strands) {
      if (!lists_created) {
        createEdgeLists();
      }
      return strands_to_neighbors.get(strands);
    }

    /**
     * Returns an immutable list of the terminals for outgoing or incoming edges.
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
     * Returns an immutable set of the terminals for outgoing or incoming edges.
     * @param strand: Which strand in this node to consider.
     * @param direction: Direction of the edge to consider.
     */
    public Set<EdgeTerminal>  getEdgeTerminalsSet(
        DNAStrand strand, EdgeDirection direction) {
      if (!lists_created) {
        createEdgeLists();
      }
      Set<EdgeTerminal> terminals;
      if (strand == DNAStrand.FORWARD) {
        if (direction == EdgeDirection.OUTGOING) {
          terminals = fOutgoingEdgeSet;
        } else {
          terminals = fIncomingEdgeSet;
        }
      } else {
        if (direction == EdgeDirection.OUTGOING) {
          terminals = rOutgoingEdgeSet;
        } else {
          terminals = rIncomingEdgeSet;
        }
      }
      return terminals;
    }

    /**
     * Returns an unmodifiable view of all of the tags for which this terminal
     * came from.
     * @param strand: Which strand of this node to get the tags for.
     * @param terminal: The other end of the edge for which we want the tags.
     * @return: An unmodifiable list of the tags for these edges.
     */
    public List<CharSequence> getTagsForEdge(
        DNAStrand strand, EdgeTerminal terminal) {
      if (!lists_created) {
        createEdgeLists();
      }

      if (strand == DNAStrand.FORWARD) {
        return f_edge_tags_map.get(terminal);
      } else {
        return r_edge_tags_map.get(terminal);
      }
    }

    /**
     * Clear any precomputed data. This function needs to be called
     * whenever the graph changes so that we don't return stale data;
     */
    public void clear() {
      // TODO(jlewi): We should probably just clear the lists rather than
      // setting them to null so as to avoid the cost of recreating the objects.
      lists_created = false;
      f_outgoing_edges = null;
      r_outgoing_edges = null;
      f_incoming_edges = null;
      r_incoming_edges = null;
      strands_to_neighbors = null;
      f_edge_tags_map = null;
      r_edge_tags_map = null;
      fOutgoingEdgeSet = null;
      rOutgoingEdgeSet = null;
      fIncomingEdgeSet = null;
      rIncomingEdgeSet = null;
      neighborIds = null;
    }

    /**
     * Return a modifiable set of the ids for the neighbors.
     *
     * We return a modifiable list of the neighborids so that GraphNodeData
     * can modify it; e.g in addNeighbor.
     *
     * @return
     */
    public HashSet<String> getNeighborIds() {
      if (neighborIds == null) {
        neighborIds = new HashSet<String>();
        for (NeighborData neighbor : this.data.getNeighbors()) {
          neighborIds.add(neighbor.getNodeId().toString());
        }
      }
      return neighborIds;
    }

    public void setNeighborIds(HashSet<String> idSet) {
      neighborIds = idSet;
    }
  }

  protected DerivedData derived_data;

  /**
   * Construct a new object with a new GraphNodeData to store the data.
   */
  public GraphNode() {
    data = new GraphNodeData();
    derived_data = new DerivedData(data);
    // Initialize any member variables so that if we serialize this instance of
    // GraphNodeData we don't have null members which causes exceptions.
    data.setR5Tags(new ArrayList<R5Tag>());
    data.setNeighbors(new ArrayList<NeighborData>());
    data.setNodeId("");

    GraphNodeKMerTag tag = new GraphNodeKMerTag();
    tag.setReadTag("");
    tag.setChunk(0);
    data.setMertag(tag);
  }

  /**
   * Construct a new node with a reference to the passed in data.
   */
  public GraphNode(GraphNodeData graph_data) {
    data = graph_data;
    derived_data = new DerivedData(data);
  }

  /**
   * Make a copy of the object.
   */
  public GraphNode clone() {
    // TODO(jlewi): The preferred way to make copies of avro records is
    // GraphNodeData data = GraphNodeData.newBuilder(value).build();
    // Unfortunately, there are a couple issues with avro that prevent this code
    // from working fully and necessitate some gymnastics.
    // 1. build is decorated with @override but overrides a method defined in
    //    an interface and not a class. This appears to be allowed in java 1.7
    //    but causes the following runtime error in earlier versions:
    // The method build() of type GraphNodeData.Builder must override a superclass method
    // see http://stackoverflow.com/questions/2335655/why-is-javac-failing-on-override-annotation
    //
    //    as a workaround we use SpecificData.deepCopy
    // 2. When build copies a ByteBuffer it tries to read all of the bytes
    //    in the buffer (i.e. capacity) instead of respecting limit.
    //    this causes an underflow exception.
    //
    //    To work around this issue with the bytebuffer we handle the
    //    compressed sequence separately. So we set the field to null
    //    during the copy and then reset it and manually copy it.
    //
    //    See https://issues.apache.org/jira/browse/AVRO-1045
    //    Should be fixed in Avro 1.7.0
    //
    // 3. The Builder API in avro has some inefficiencies see:
    //    https://issues.apache.org/jira/browse/AVRO-985
    //    https://issues.apache.org/jira/browse/AVRO-989
    //
    //    We should see whether we are affected by these issues. In several
    //    stages, e.g QuickMerge, we will make copies of all nodes so
    //    making the copy efficient will be a big win.
    //
    // We can work around all these issues by implementing our own copy method.
    CompressedSequence sequence = data.getSequence();
    data.setSequence(null);

    GraphNodeData copy = (GraphNodeData)
        SpecificData.get().deepCopy(data.getSchema(), data);

    CompressedSequence sequence_copy = new CompressedSequence();
    copy.setSequence(sequence_copy);
    sequence_copy.setLength(sequence.getLength());

    ByteBuffer source_buffer = sequence.getDna();
    byte[] buffer = Arrays.copyOf(
        sequence.getDna().array(), sequence.getDna().array().length);
    sequence_copy.setDna(ByteBuffer.wrap(
        buffer, 0, source_buffer.limit()));

    // Reset the sequence.
    data.setSequence(sequence);
    return new GraphNode(copy);
  }
  /**
   * Add information about a destination KMer which came from the start of a read.
   *
   * If the destination KMer came from the start of a read, then the edge
   * Source KMer->Destination KMer edge allows us to connect the two reads.
   *
   * @param tag - String identifying the read from where the destination KMer came
   *              from.
   * @param offset - 0 if isRC is false, K-1 otherwise.
   * @param strand - The strand that is aligned with the start of the read.
   * @param maxR5 - Maximum number of r5 tags to store.
   */
  public void addR5(KMerReadTag tag, int offset, DNAStrand strand, int maxR5)
  {

    if (data.r5_tags.size() < maxR5)
    {
      R5Tag val = new R5Tag();

      val.tag = tag.toString();
      val.offset = offset;
      val.setStrand(strand);
      data.r5_tags.add(val);
    }
  }

  /**
   * Add the neighbor to this node.
   *
   * The node steals the reference to neighbor data so the caller shouldn't
   * modify the neighbor data.
   *
   * Warning. This function should only be used if you know what you are doing.
   * In general you should  use addIncomingEdge/addOutgoingEdge to add eges
   * to the node.
   *
   * @param neighbor
   */
  public void addNeighbor(NeighborData neighbor) {
    // Ensure the node doesn't already have a neighbor for this node.
    if (this.derived_data.getNeighborIds().contains(
          neighbor.getNodeId().toString())) {
      throw new RuntimeException(String.format(
          "Tried to add neighbor %s to node %s but neighbor already exists.",
          neighbor.getNodeId(), getNodeId()));
    }

    // Minimal validation, check that fields aren't null so that we can
    // serialize it.
    if (neighbor.getNodeId() == null) {
      throw new RuntimeException("neighbor has null fields");
    }
    if (neighbor.getEdges() == null) {
      throw new RuntimeException("neighbor has null fields");
    }
    for (EdgeData edgeData : neighbor.getEdges()) {
      if (edgeData.getStrands() == null) {
        throw new RuntimeException("edgeData has null fields");
      }
      if (edgeData.getReadTags() == null) {
        throw new RuntimeException("edgeData has null fields");
      }
    }
    // Make a copy of the neighborIds.
    HashSet<String> neighborIds = this.derived_data.getNeighborIds();
    neighborIds.add(neighbor.getNodeId().toString());
    this.derived_data.clear();
    this.derived_data.setNeighborIds(neighborIds);
    this.data.getNeighbors().add(neighbor);
  }

  /**
   * Set the mertag based on the read tag for a KMer.
   * @param mertag
   */
  public void setMertag(KMerReadTag mertag) {
    // TODO(jlewi): Do we really need this method?
    GraphNodeKMerTag tag = new GraphNodeKMerTag();
    tag.setReadTag(mertag.read_id);
    tag.setChunk(mertag.chunk);
    data.setMertag(tag);
  }

  public void setCoverage(float cov) {
    data.setCoverage(cov);
  }

  /**
   * Set the canonical sequence represented by this node.
   * @param seq
   */
  public void setSequence(Sequence seq) {
    CompressedSequence compressed = new CompressedSequence();
    compressed.setDna(ByteBuffer.wrap(seq.toPackedBytes(), 0, seq.numPackedBytes()));
    compressed.setLength(seq.size());
    data.setSequence(compressed);
  }

  /**
   * @param seq
   */
  public void setSequence(ByteBuffer seq, int length) {
    CompressedSequence compressed = new CompressedSequence();
    compressed.setDna(seq);
    compressed.setLength(length);
    data.setSequence(compressed);
  }

  /**
   * Return a reference to the GraphNodeData object storing all the data.
   * @return
   */
  public GraphNodeData getData() {
    return data;
  }

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
   * Find the strand of node that has an edge to terminal.
   *
   * This function is deprecated because it improperly assumes each node
   * cane haveonly one strand could be connected to a given edge terminal.
   * However, there are cases such as palindromes where this may not be true.
   * If this assumption is not true we raise an exception. The correct thing to
   * do is use getEdgeTerminalsSet and then check if each strand contains
   * the terminal in question.
   *
   * @param terminal: The terminal to find the edge to.
   * @param direction: The direction for the edge.
   * @returns The strand or null if no edge exists.
   */
  @Deprecated
  public DNAStrand findStrandWithEdgeToTerminal(
      EdgeTerminal terminal, EdgeDirection direction) {
    Set<DNAStrand> strands = findStrandsWithEdgeToTerminal(terminal, direction);
    if (strands.size() > 1) {
      throw new RuntimeException(
          "The forward and reverse strand both have edges to the terminal " +
          "the function findStrandWithEdgeTerminal erroneously assumes that " +
          "only one strand has an edge to the terminal.");
    }

    if (strands.size() == 1) {
      return strands.iterator().next();
    }
    return null;
  }

  /**
   * Find the strands of this node that have an edge to terminal.
   *
   * @param terminal: The terminal to find the edge to.
   * @param direction: The direction for the edge.
   * @returns The strand or null if no edge exists.
   */
  public Set<DNAStrand> findStrandsWithEdgeToTerminal(
      EdgeTerminal terminal, EdgeDirection direction) {
    HashSet<DNAStrand> strands = new HashSet<DNAStrand>();
    for (DNAStrand strand : DNAStrand.values()) {
      if (this.getEdgeTerminalsSet(strand, direction).contains(terminal)) {
        strands.add(strand);
      }
    }
    return strands;
  }


  /**
   * Add an outgoing edge to this node.
   * @param strand: Which strand to add the edge to.
   * @param dest: The destination
   * @param tags: (Optional): List of strings identifying the reads where this
   *   edge came from.
   * @param MAXTHREADREADS: The tag will only be recorded if we have fewer
   *   than this number of tags associated with this read.
   */
  public void addOutgoingEdgeWithTags(DNAStrand strand, EdgeTerminal dest,
      Collection<? extends CharSequence> tags,
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

    if (tags !=null) {
      if (edge.getReadTags().size() < MAXTHREADREADS){
        List<CharSequence> edge_tags = edge.getReadTags();
        long max_insert = MAXTHREADREADS - edge.getReadTags().size();
        long num_to_insert = max_insert > tags.size() ? tags.size() :
          max_insert;
        Iterator<? extends CharSequence> tagIterator = tags.iterator();
        for (int i = 0 ; i < num_to_insert; i++) {
          edge_tags.add(tagIterator.next());
        }
      }
    }
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
    List<CharSequence> tags = new ArrayList<CharSequence>();
    tags.add(tag);
    addOutgoingEdgeWithTags(strand, dest, tags, MAXTHREADREADS);
  }

  /**
   * Add an outgoing edge to this node.
   * @param strand: Which strand to add the edge to.
   * @param dest: The destination
   */
  public void addOutgoingEdge(DNAStrand strand, EdgeTerminal dest) {
    addOutgoingEdgeWithTags(strand, dest, null, 0);
  }

  /**
   * Add an incoming edge to this node.
   * @param strand: Which strand to add the edge to.
   * @param src: The destination
   * @param tags: (Optional): List of strings identifying the reads where this
   *   edge came from.
   * @param MAXTHREADREADS: The tag will only be recorded if we have fewer
   *   than this number of tags associated with this read.
   */
  public void addIncomingEdgeWithTags(DNAStrand strand, EdgeTerminal src,
      List<CharSequence> tags,
      long MAXTHREADREADS) {
    // Let this node be X.
    // Then edge Y->X implies edge RC(x)->RC(y).
    // So we add RC(x)->RC(y) to this node.
    EdgeTerminal dest = new EdgeTerminal(
        src.nodeId, DNAStrandUtil.flip(src.strand));
    addOutgoingEdge(DNAStrandUtil.flip(strand), dest);
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
    EdgeTerminal dest = new EdgeTerminal(
        src.nodeId, DNAStrandUtil.flip(src.strand));
    addOutgoingEdge(DNAStrandUtil.flip(strand), dest);
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
   * in a particular direction
   **/
  public int degree(DNAStrand strand, EdgeDirection direction)  {
    return getEdgeTerminals(strand, direction).size();
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

  public void setNodeId(String nodeid) {
    data.setNodeId(nodeid);
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
   * A partial string representation that is primarily intended for displaying
   * in a debugger. This representation is likely to change and it should
   * not be relied upon for any processing.
   */
  public String toString() {
    String represent = "Id:" + getNodeId() + " ";
    represent += "Sequence:" + this.getSequence().toString();
    represent += " F_EDGES: ";
    for (EdgeTerminal edge:
      this.getEdgeTerminals(DNAStrand.FORWARD, EdgeDirection.OUTGOING)) {
      represent += edge.toString() + ",";
    }
    represent += " R_EDGES: ";
    for (EdgeTerminal edge:
      this.getEdgeTerminals(DNAStrand.REVERSE, EdgeDirection.OUTGOING)) {
      represent += edge.toString() + ",";
    }
    return represent;
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

  /**
   * Returns an immutable set of the terminals for outgoing or incoming edges.
   * @param strand: Which strand in this node to consider.
   * @param direction: Direction of the edge to consider.
   */
  public Set<EdgeTerminal>  getEdgeTerminalsSet(
      DNAStrand strand, EdgeDirection direction) {
    return derived_data.getEdgeTerminalsSet(strand, direction);
  }

  /**
   * Returns an unmodifiable view of all of the tags for which this terminal
   * came from.
   * @param strand: Which strand of this node to get the tags for.
   * @param terminal: The other end of the edge for which we want the tags.
   * @return: An unmodifiable list of the tags for these edges.
   */
  public List<CharSequence> getTagsForEdge(
      DNAStrand strand, EdgeTerminal terminal) {
    return derived_data.getTagsForEdge(strand, terminal);
  }

  /**
   * Return the canonical sequence for this node.
   * @return
   */
  public Sequence getSequence() {
    Sequence sequence = new Sequence(DNAAlphabetFactory.create());
    byte[] bytes = data.getSequence().getDna().array();
    int length = data.getSequence().getLength();
    sequence.readPackedBytes(bytes, length);
    return sequence;
  }

  /**
   * Return the coverage.
   */
  public float getCoverage() {
    return this.data.getCoverage();
  }

  /**
   * Move the outgoing edge.
   *
   * This function is used when the terminal for an outgoing edge gets merged
   * with other nodes. Thus, we need to update the nodeid and strand for this
   * terminal
   * @param strand: Which strand the edge is on.
   * @param old_terminal: The old terminal
   * @param new_terminal: The new terminal
   */
  public void moveOutgoingEdge(
      DNAStrand strand, EdgeTerminal old_terminal, EdgeTerminal new_terminal) {
    NeighborData old_neighbor = findNeighbor(old_terminal.nodeId);

    if (old_neighbor == null) {
      throw new RuntimeException(
          "Could not find a neighbor with id:" + old_terminal.nodeId);
    }

    if (old_terminal.equals(new_terminal)) {
      // Note: It is possible that the nodeid is the same but the strand
      // is different.
      throw new RuntimeException("New node is the same as the old node");
    }

    NeighborData new_neighbor = findNeighbor(new_terminal.nodeId);
    if (new_neighbor == null) {
      new_neighbor = new NeighborData();
      new_neighbor.setNodeId(new_terminal.nodeId);
      new_neighbor.setEdges(new ArrayList<EdgeData>());
      data.getNeighbors().add(new_neighbor);
    }

    // Find the edge data.
    StrandsForEdge old_strands = StrandsUtil.form(strand, old_terminal.strand);
    EdgeData old_edgedata = findEdgeDataForStrands(old_neighbor, old_strands);
    if (old_edgedata == null) {
      throw new RuntimeException(
          "Could not find edge data for the edge being moved");
    }

    // Find the new edge data.
    StrandsForEdge new_strands = StrandsUtil.form(strand, new_terminal.strand);
    EdgeData new_edgedata = findEdgeDataForStrands(new_neighbor, new_strands);
    if (new_edgedata == null) {
      new_edgedata = new EdgeData();
      new_edgedata.setStrands(new_strands);
      new_edgedata.setReadTags(new ArrayList<CharSequence>());
      new_neighbor.getEdges().add(new_edgedata);
    }

    // Copy the data.
    new_edgedata.getReadTags().addAll(old_edgedata.getReadTags());

    // Remove this data from the old neighbor.
    removeEdgesForNeighbor(old_neighbor, old_strands);

    for (int index = 0; index < old_neighbor.getEdges().size(); index++) {
      if (old_neighbor.getEdges().get(index).getStrands() == old_strands) {
        old_neighbor.getEdges().remove(index);
        break;
      }
    }

    // Remove old_neighbor if there are no more edges to it.
    if (old_neighbor.getEdges().size() == 0) {
      removeNeighbor(old_neighbor.getNodeId().toString());
    }

    // Clear the derived data because it is invalid.
    derived_data.clear();
  }

  /**
   * Remove the edge for the given strands from the given neighbor.
   * Neighbor should be a reference to data inside this.data.
   * We don't check to verify this.
   *
   * @return: True on success false otherwise.
   */
  protected boolean removeEdgesForNeighbor(
      NeighborData neighbor, StrandsForEdge strands) {
    for (int index = 0; index < neighbor.getEdges().size(); index++) {
      if (neighbor.getEdges().get(index).getStrands() == strands) {
        // We assume that each instance of strands appears at most once.
        // so after removing it we can just return.
        neighbor.getEdges().remove(index);
        derived_data.clear();
        return true;
      }
    }
    return false;
  }

  /**
   * Remove the neighbor with the given id from this node.
   *
   * This function returns the data for the neighbor. This is useful if you
   * want to manipulate the data for this neighbor without going through the
   * interface provided by GraphNode.
   *
   * @return: The data for the removed neighbor, null if neighbor doesn't exist.
   */
  public NeighborData removeNeighbor(String neighborid) {
    NeighborData neighbor = null;
    for (int index = 0; index < data.getNeighbors().size(); index++) {
      if (data.getNeighbors().get(index).getNodeId().toString().equals(
          neighborid)) {
        neighbor = data.getNeighbors().get(index);
        // We assume that each instance of a neighbor appears at most once.
        // so after removing it we can just return.
        data.getNeighbors().remove(index);
        derived_data.clear();
        return neighbor;
      }
    }
    return neighbor;
  }
}
