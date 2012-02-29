package contrail.graph;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import contrail.avro.R5Tag;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * Some utilities for working with R5Tags.
 *
 */
public class R5TagUtil {

  /**
   * Return the prefix of the read that is aligned with this sequence
   * based on R5Tag. 
   *  
   * 
   * @param canonical
   * @param tag
   * @return
   */
  public static Sequence prefixForTag(Sequence src, R5Tag tag) {
    // Warning: This function is inefficient because
    // we potentially compute the reverse complement several times.
    
    // R5Tag is always relative to the canonical strands so
    // we fetch the canonical version.
    Sequence canonical = DNAUtil.canonicalseq(src);
    src = DNAUtil.canonicalToDir(src, tag.getStrand());
    
    int offset = tag.getOffset();
    if (tag.getStrand() == DNAStrand.REVERSE) {
      offset = canonical.size() - offset -1;
    }
    
    return src.subSequence(offset, src.size());        
  }
  
  // TODO(jlewi): Figure out what to do with this code.
  /**
   * Look at the read alignment tags for the node and return true
   * if the node is at the start and end of a read.
   * @param node
   * @return
   */
//  protected static boolean nodeIsAtEndsOfRead(List<R5Tag> tags) {
//    // We do this as follows, group the tags by the tag for the read.
//    // Then process each tag to see if it is aligned with both ends.
//    Hashtable<String, List<R5Tag>> tags_map;
//    for(R5Tag tag: tags) {
//      List<R5Tag> these_tags;
//      these_tags = tags_map.get(tags_map.get(tag));
//      if (these_tags == null) {
//        these_tags = new ArrayList<R5Tag>();
//        tags_map.put(tag.getTag().toString(), these_tags);
//      }
//      these_tags.add(tag);
//    }
//    
//    need to finish this function
//    
//  }
}
