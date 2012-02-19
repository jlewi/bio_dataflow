package contrail.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import contrail.ContrailConfig;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

public class NodeMerger {


  /**
   * Copy edges from old_node to new node. 
   * @param new_node: The node to copy to.
   * @param old_node: The node to copy from.
   * @param strand: Which strand of the node we are considering.
   * @param direction: Whether to consider  incoming or outgoing edges.
   */
  protected void copyEdgesForStrand(
      GraphNode new_node, DNAStrand new_strand, GraphNode old_node,
      DNAStrand old_strand,
      EdgeDirection direction) {
    
    List<EdgeTerminal> terminals = 
      old_node.getEdgeTerminals(old_strand, direction);
      
    
    for (Iterator<EdgeTerminal> it = terminals.iterator();
         it.hasNext();) {
      EdgeTerminal terminal = it.next();
      
      // TODO(jlewi): We need to get the tags. 
      List<CharSequence> tags = old_node.getTagsForEdge(old_strand, terminal);

      // I got confused; we only shift the R5Tags.
//      List<CharSequence> shifted_tags = new ArrayList<CharSequence>();
//      for (CharSequence tag: tags) {
//        String [] vals = tag.toString().split(":");
//        int offset = Integer.parseInt(vals[1]) + shift;
//        shifted_tags.add(vals[0] + ":" + offset);
//      }
      new_node.addIncomingEdgeWithTags(
          new_strand, terminal, tags, ContrailConfig.MAXTHREADREADS);
    }
  }
  
//  protected void addOutgoingEdgesForStrand(
//        GraphNode new_node, GraphNode src, 
//        DNAStrand strand, int shift) {
//      
//            
//      List<EdgeTerminal> terminals = 
//        src.getEdgeTerminals(strand, EdgeDirection.OUTGOING);
//        
//      
//      for (Iterator<EdgeTerminal> it = terminals.iterator();
//           it.hasNext();) {
//        EdgeTerminal terminal;
//        
//        // TODO(jlewi): We need to get the tags. 
//        List<CharSequence> tags = src.getTagsForEdge(strand, terminal);
//        List<CharSequence> shifted_tags;
//        for (CharSequence tag: tags) {
//          String [] vals = tag.toString().split(":");
//          int offset = Integer.parseInt(vals[1]) + shift;
//          shifted_tags.add(vals[0] + ":" + offset);
//        }
//        new_node.addOutgoingEdge(strand, terminal, shifted_tags);
//      }
//  }
//  protected List<EdgeTerminal> addEdgesForStrand(
//      GraphNode new_node, GraphNode src, GraphNode dest, 
//      StrandsForEdge strands) {
//    
//    addIncomingEdgesForStrand(new_node, src, dest, strands); 
//        
//    
//    List<EdgeTerminal> outgoing_edges = 
//        src.getEdgeTerminals(StrandsUtil.src(strands), EdgeDirection.INCOMING);
//  }

  /**
   * Make a copy of the R5Tags. 
   * @param tags
   * @return
   */
  protected List<R5Tag> copyR5Tags(List<R5Tag> tags) {       
    List<R5Tag> new_list = new ArrayList<R5Tag>();
    for (R5Tag tag: tags) {      
      R5Tag copy = new R5Tag();
      copy.setTag(tag.getTag());
      copy.setStrand(tag.getStrand());
      copy.setOffset(tag.getOffset());
      new_list.add(copy);
    }
    return new_list;
  }
  
  /**
   * Reverse a list of R5 tags in place. 
   * @param tags
   * @return
   */
  protected void reverseReads(List<R5Tag> tags, int length) {       
    for (R5Tag tag: tags) {      
      tag.setStrand(DNAUtil.flip(tag.getStrand()));
      tag.setOffset(length - tag.getOffset() -1);
    }
  }
  
  /**
   * Datastructure for returning information about the merging of
   * two sequences.
   * 
   * @author jlewi
   *
   */
  protected class MergeInfo {
    /**
     * This is the canonical representation of the merged sequences.
     */
    public Sequence canonical_merged;
    
    /**
     * This indicates which strand the merged sequence came from. 
     */
    public DNAStrand merged_strand;   
    
    /**
     * Whether we need to reverse the read tags.
     */
    public boolean reverse_reads;
    
  }
  
  /**
   * Merge two sequences. This is not really a public function. Its
   * @param strands: Which strands to do the merge along. 
   */
  protected MergeInfo mergeSequences(
      Sequence canonical_src, Sequence canonical_dest, 
      StrandsForEdge strands, int overlap) {
    MergeInfo info = new MergeInfo();
    
    Sequence src_sequence = DNAUtil.canonicalToDir(
        canonical_src, StrandsUtil.src(strands));
    
    Sequence dest_sequence = DNAUtil.canonicalToDir(
        canonical_dest, StrandsUtil.dest((strands)); 
        
      //dest.getCanonicalSequence();
    
    //DNAStrand original_src_strand = DNAUtil.canonicaldir(src_sequence);
    
    // Check the overlap.
    // TODO(jlewi): We could probably make this comparison more efficient.
    // Copying the data to form new sequences is probably inefficient.
    Sequence src_overlap = src_sequence.subSequence(
        src_sequence.size() - overlap, src_sequence.size());
    Sequence dest_overlap = src_sequence.subSequence(0, overlap);

    if (!src_overlap.equals(dest_overlap)) {
      throw new RuntimeException(
          "Can't merge nodes. Sequences don't overlap by: " + overlap + 
          "bases.");
    }
    
    // Combine the sequences.
    Sequence dest_nonoverlap = dest_sequence.subSequence(
        overlap, dest_sequence.size());
    src_sequence.add(dest_nonoverlap);
    
    info.canonical_merged = DNAUtil.canonicalseq(src_sequence);
    info.merged_strand = DNAUtil.canonicaldir(src_sequence);
    
    // Determine whether we need to reverse the reads. 
    // We need to reverse the reads if the direction of the merged
    // sequence and the original src_sequence don't match    
    if (info.merged_strand != StrandsUtil.src(strands)) {
      info.reverse_reads = true;
    } else {
      info.reverse_reads = false;
    }
    return info;
  }
  
  /**
   * Merge two nodes
   * @param src: The source node
   * @param dest: The destination node
   * @param strands: Which strands the edge from src->dest comes from.
   * @param overlap: The number of bases that should overlap between
   *   the two sequences.
   * @return
   * @throws RuntimeException if the nodes can't be merged.
   */
  public GraphNode mergeNodes(
      GraphNode src, GraphNode dest, StrandsForEdge strands, int overlap) {
    // To merge two nodes we need to
    // 1. Form the combined sequences
    // 2. Update the coverage
    // 3. Remove outgoing edges from src.
    // 4. Remove Incoming edges to dest
    // 5. Add Incoming edges to src
    // 6. Add outgoing edges from dest
    
    Sequence src_sequence = src.getCanonicalSequence();
    Sequence dest_sequence = dest.getCanonicalSequence();
    MergeInfo merge_info = mergeSequences(
        src_sequence, dest_sequence, strands);
    
    GraphNode new_node = new GraphNode();
    new_node.setCanonicalSequence(canonical_sequence);
    
    // Preserve the edges we need to preserve the incoming/outgoing
    // edges corresponding to the strands but we also need to consider
    // the reverse complement of the strand.
    // Add the incoming edges.
    copyEdgesForStrand(new_node, new_strand, src, StrandsUtil.src(strands),
                       EdgeDirection.INCOMING);
    // add the outgoing edges.
    copyEdgesForStrand(new_node, new_strand, dest, StrandsUtil.dest(strands),
        EdgeDirection.OUTGOING);
    
    
    // Now add the incoming and outgoing edges for the reverse complement.
    DNAStrand rc_strand = DNAStrandUtil.flip(new_strand);
    StrandsForEdge rc_edge_strands = StrandsUtil.complement(strands);
    copyEdgesForStrand(new_node, rc_strand, src, 
        StrandsUtil.src(rc_edge_strands),
        EdgeDirection.INCOMING);

    // add the outgoing edges.
    copyEdgesForStrand(new_node, rc_strand, dest, 
        StrandsUtil.dest(rc_edge_strands),
        EdgeDirection.OUTGOING);    
    
    // Update the read alignment tags (R5Fields).
    // Make a copy of src tags.
    List<R5Tag> src_tags = copyR5Tags(src.getData().getR5Tags());
    
    // Reverse the reads if necessary.
    if (new_strand == DNAStrand.FORWARD) {
      // We don't need to reverse the reads.
      
    }
    
    for (R5Tag rtag: src.getData().getR5Tags()) {
      if (rtag.)
    }
  }
}
