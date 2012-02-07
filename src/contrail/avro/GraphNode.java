package contrail.avro;

import contrail.CompressedSequence;
import contrail.DestForLinkDir;
import contrail.EdgeDestNode;
import contrail.GraphNodeData;
import contrail.GraphNodeKMerTag;
import contrail.R5Tag;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.tools.ant.taskdefs.Java;


/**
 * Wrapper class for accessing modifying the GraphNodeData structure.
 * 
 * This class is used to represent the output of BuildGraphAvro.
 * The class represents a node (KMer) in the graph and all outgoing
 * edges from that sequence
 * 
 * @author jlewi
 *
 */
public class GraphNode {

  private GraphNodeData data;

  /**
   * Class is a container for all the derived data for the node that
   * we want to avoid recomputing each time it is accessed.
   */
  protected static class DerivedData {
    public String nodeid_m;
    
    private GraphNodeData data;
    public DerivedData(GraphNodeData data) {
      this.data = data;
    }
    /**
     * This hash map maps the two letter direction for an edge e.g "fr"
     * to a list of strings which are the node ids for the destination nodes
     */
    private HashMap<CharSequence, List<CharSequence>> 
        linkdirs_to_dest_nodeid;
    
    public List<CharSequence> getDestNodeIdsForLinkDir(String link_dir) {
      if (linkdirs_to_dest_nodeid == null) {
        linkdirs_to_dest_nodeid = 
            new HashMap<CharSequence, List<CharSequence>> ();
        
        EdgeDestNode dest_node;
        List<CharSequence> id_list;
        // Loop over the destination nodes.
        for (Iterator<EdgeDestNode> it = data.getDestNodes().iterator();
             it.hasNext();) {
          dest_node = it.next();
          for (Iterator<DestForLinkDir> link_it = dest_node.getLinkDirs().iterator();
               link_it.hasNext();) {
            DestForLinkDir dest_for_link_dir = link_it.next();
            java.lang.CharSequence dir = dest_for_link_dir.getLinkDir();
            id_list = linkdirs_to_dest_nodeid.get(dir);
            if (id_list == null) {
              id_list = new ArrayList<CharSequence> ();
              linkdirs_to_dest_nodeid.put(dir, id_list);
            }
            id_list.add(dest_node.getNodeId());
          }
        }
      }
      return linkdirs_to_dest_nodeid.get(link_dir);
    }
  }
  
  protected DerivedData derived_data;
  
  /**
   * Valid values for the direction.
   */
  public static final String[] DIRS = {"f", "r"};
  
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
   * Add an outgoind edge to this node.
   *
   * @param link_dir - Two letter string such as 'ff' which indicates the canonical
   *   direction of the source and destination KMers. 
   * @param canonical_dest - The canonical sequence for the destination KMer.
   * @param tag - A string identifying the read (and location within the read) where this 
   *   edge came from.
   * @param MAXTHREADREADS - Maximum number of threads to record the tags for.
   */
  public void addEdge(CharSequence link_dir, Sequence canonical_dest, String tag, long MAXTHREADREADS) {
    if (data.getDestNodes() == null) {
      data.setDestNodes(new ArrayList<EdgeDestNode>());
    }
    // Get a list of the edges for this direction.
    // Create the list if it doesn't exist.
    EdgeDestNode dest_node = findDestNode(canonical_dest);
     
    if (dest_node == null) {
      dest_node = new EdgeDestNode();
      CompressedSequence compressed = new CompressedSequence();
      compressed.setDna(ByteBuffer.wrap(canonical_dest.toPackedBytes(), 0, canonical_dest.numPackedBytes()));
      compressed.setLength(canonical_dest.size());      
      dest_node.setCanonicalSequence(compressed);
      dest_node.setLinkDirs(new ArrayList<DestForLinkDir>());
      data.getDestNodes().add(dest_node);
    }
    
        
    // Check if we already have an instance for this link direction.
    DestForLinkDir link_info = findInstancesForLinkDir(dest_node, link_dir);
    
    if (link_info == null) {
      link_info = new DestForLinkDir();
      link_info.setReadTags(new ArrayList<CharSequence>());
      dest_node.getLinkDirs().add(link_info);      
    }
    
    link_info.setLinkDir(link_dir);
        
    if (link_info.getReadTags().size() < MAXTHREADREADS)
    {
      link_info.getReadTags().add(tag);
    }
  }
  
  /**
   * Find the instance of EdgeDestNodes representing the given canonical_sequence.
   * @param canonical_sequence
   * @return The instance if found or null otherwise.
   * 
   * TODO(jlewi): We could probably speed this up by creating a hash map for the 
   * destination sequences.
   */
  private EdgeDestNode findDestNode(Sequence canonical_sequence) {
    if (data.getDestNodes() == null) {
      return null;
    }
    Sequence canonical = new Sequence(DNAAlphabetFactory.create());
    for (Iterator<EdgeDestNode> it_dest_node = data.getDestNodes().iterator();
         it_dest_node.hasNext();) {
      EdgeDestNode dest = it_dest_node.next();
      canonical.readPackedBytes(dest.getCanonicalSequence().getDna().array(), 
          dest.getCanonicalSequence().getLength());
      
      if (canonical.equals(canonical_sequence)) {
        return dest;
      }
    }
    return null;   
  }
  
  /**
   * Find the edge instances inside EdgeDestNode for the given link direction.
   * 
   * @param canonical_sequence
   * @return The instance if found or null otherwise.
   * 
   * TODO(jlewi): We could probably speed this up by creating a hash map for the 
   * destination sequences.
   */
  private DestForLinkDir findInstancesForLinkDir(EdgeDestNode node, CharSequence link_dir) {
    
    if (node.getLinkDirs() == null) {
      return null;
    }
    
    for (Iterator<DestForLinkDir> it = node.getLinkDirs().iterator();
         it.hasNext();) {
      DestForLinkDir link_info = it.next();
      if (link_info.getLinkDir() == link_dir) {
        return link_info;
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
   * @param dir - The orientation direction for this kmer. Can be "r" or "f".
   * @return 
   * TODO(jlewi): Add a unittest.
   */
  public int degree(CharSequence dir) {
    int retval = 0;

    throw new RuntimeException("Need to finish implementing this function");
//    String fd = dir + "f";
//    if (fields.containsKey(fd)) { retval += fields.get(fd).size(); }
//
//    String rd = dir + "r";
//    if (fields.containsKey(rd)) { retval += fields.get(rd).size(); }
  
    //return retval;
  }
  
  public String getNodeId() {
    // TODO(jlewi): nodeid should probably be part of the data schema.
    return derived_data.nodeid_m; 
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
  public List<CompressedSequence> getCanonicalDestForLinkDir(String key) {
    throw new RuntimeException("Need to implement this method");
  }
  
  /**
   * Return the tail information for this node. If this node has out degree
   * 1 then its tail is the node we are connected to.
   * 
   * @param dir - The read direction for the source kmer for which we consider 
   *   tail information.
   * @return - An instance of Tailinfo. ID is set to the  
   *  representation of the destination node,
   *   dist is initialized to 1, and dir is the direction coresponding to the 
   *   destination node.
   *   
   * TODO(jlewi): Add a unittest. 
   * TODO(jlewi): Clean up the docstring once the code is finalized. 
   */
  public TailInfoAvro gettail(CharSequence dir)
  {    
    if (degree(dir) != 1)
    {
      return null;
    }
    
    TailInfoAvro ti = new TailInfoAvro();
    ti.dist = 1;
    
    // Since the outdegree is 1 we either have 1 edge in direction
    // dir + "f" or 1 edge in direction dir + "r" 
    String fd = dir + "f";
    List<CharSequence> dest_ids = getDestIdsForSrcDir(fd);
    if (dest_ids != null)  { 
      ti.id = dest_ids.get(0);  
      ti.dir = "f";
    } else {
      fd = dir + "r";
      dest_ids = getDestIdsForSrcDir(fd);
      ti.id = dest_ids.get(0);
      ti.dir = "r";
    }
    return ti;
  }
  
  /** 
   * Return a list of strings containing the ids of the destination nodes 
   * for links corresponding to the two letter link direction given by link_dir.
   * @param link_dir
   * @return
   */
  public List<CharSequence> getDestIdsForSrcDir(String link_dir) {
    return derived_data.getDestNodeIdsForLinkDir(link_dir);
  }
  
  /**
   * Return a list of all the ids for destination nodes.
   */
  public List<CharSequence> getAllDestIds() {
    throw new NotImplementedException();
  }
}
