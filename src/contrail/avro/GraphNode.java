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
   *     caonical version of the KMer.
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
}
