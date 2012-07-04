package contrail.graph;

import contrail.graph.R5Tag;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * Some utilities for working with R5Tags.
 *
 */
public class R5TagUtil {

  /**
   * Return the prefix of the read that is aligned with this sequence
   * based on R5Tag. The strand in tag is always interpreted relative
   * to the strand in src.
   * @param canonical
   * @param tag
   * @return
   */
  public static Sequence prefixForTag(Sequence src, R5Tag tag) {
    // Warning: This function is inefficient because
    // we potentially compute the reverse complement several times.
    // R5Tag is always relative to the canonical strands so
    // we fetch the canonical version.
    src = DNAUtil.sequenceToDir(src, tag.getStrand());

    int offset = tag.getOffset();
    if (tag.getStrand() == DNAStrand.REVERSE) {
      offset = src.size() - offset -1;
    }

    return src.subSequence(offset, src.size());
  }

  /**
   * Reverse a tag in place so that is relative to the reverse complement of
   * the current strand.
   * @param tag: The tag
   * @param length: The length of the DNAStrand.
   */
  public static void reverse(R5Tag tag, int length) {
    tag.setStrand(DNAStrandUtil.flip(tag.getStrand()));
    tag.setOffset(length - tag.getOffset() -1);
  }
}
