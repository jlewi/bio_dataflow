package contrail.graph;

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
}
