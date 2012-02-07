package contrail.avro;

import contrail.ContrailConfig;
import contrail.KMerEdge;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * Class for manipulating edges in the DeBruijn graph.
 * 
 * Edges represent two KMers which overlap for K-1 bases.
 * 
 * TODO (jlewi): What should the memory model be? Should we own the data?
 * 
 * TODO(jlewi): We can probably remove this class; I don't think its used anywhere.
 *
 */
public class Edge {

  private Sequence src_seq = null;
  private Sequence dest_seq = null;
  /**
   * Construct the edge from the data in a KMerEdge object.
   * 
   * TODO(jlewi): Add unittest.
   * TODO(jlewi): We could probably make this function more efficient.
   * 
   * @param source_bytes - Bytes representing the compressed, canonical version of
   *   the source KMer.
   * @param data - Stores the data for the edge. We steal a reference to this
   *   buffer.
   * @param length - The length of the KMers.
   * @return
   */
  static public Edge fromKMerEdge(byte[] source_bytes, KMerEdge data, int length) {
    Edge edge = new Edge();
    Sequence src_seq = new Sequence(DNAAlphabetFactory.create(), length);        
    src_seq.readPackedBytes(source_bytes, length);
    
    CharSequence link_dir = data.getLinkDir();
    int pos = 0;
    if (link_dir.charAt(pos) == 'f') {
      edge.src_seq = src_seq;
    }
    else if (link_dir.charAt(pos) == 'r') {
      edge.src_seq = DNAUtil.reverseComplement(src_seq);
    }
    else {
      throw new RuntimeException("Unrecognized link direction: " + link_dir.charAt(pos));      
    }
    
    // TODO(jlewi): Can we eliminate the extra copy?
    edge.dest_seq = src_seq.subSequence(1, length);
    pos = 1;
    if (link_dir.charAt(pos) == 'f') {
      // Do nothing
    }
    else if (link_dir.charAt(pos) == 'r') {
      edge.dest_seq = DNAUtil.reverseComplement(edge.dest_seq);
    }
    else {
      throw new RuntimeException("Unrecognized link direction: " + link_dir.charAt(pos));      
    }
    
    Sequence last_base_seq = new Sequence(DNAAlphabetFactory.create(), 1);
    last_base_seq.readPackedBytes(data.getLastBase().array(), 1);
    edge.dest_seq.add(last_base_seq);
    
    return edge;
  }
}
