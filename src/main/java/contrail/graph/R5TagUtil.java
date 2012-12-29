package contrail.graph;

import java.util.Collection;
import java.util.HashSet;

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

//  /**
//   * Flip the offset.
//   *
//   * @param offset
//   * @param length:
//   * @return
//   */
//  public static int flipOffset(int offset, int length) {
//    return length - offset -1;
//  }
//
//  /**
//   * Return the position of the aligned strand in which the tag starts.
//   *
//   * @param tag
//   * @param length: Length of the sequence.
//   * @return
//   */
//  public static int prefixStart(R5Tag tag, int length) {
//    if (tag.getStrand() == DNAStrand.FORWARD) {
//      return tag.getOffset();
//    }
//    return length - tag.getOffset() -1;
//  }

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

  /**
   * Convert a list of R5Tags to a set of strings
   */
  private static HashSet<String> convertR5TagsToStrings (
      Collection<R5Tag> tags) {
    HashSet<String> tagSet = new HashSet<String>();
    String value = null;
    for (R5Tag item : tags ) {
      value = String.format(
          "%s:%d:%d", item.getTag(), item.getStrand(), item.getOffset());
      if (tagSet.contains(value)) {
        throw new RuntimeException("List of R5 Tags has duplicates");
      }
    }
    return tagSet;
  }

  /**
   * Return true is the two sets of R5Tags are equal irrespective of order.
   *
   * The function assumes neither list has duplicates; if there are duplicates
   * we throw an exception.
   *
   * Neither list should be null. If the lists are null the behavior is
   * undefined.
   */
  public static boolean listsAreEqual(
      Collection<R5Tag> left, Collection<R5Tag> right) {
    if (left.size() !=  right.size()) {
      return false;
    }
    HashSet<String> leftSet = convertR5TagsToStrings(left);
    HashSet<String> rightSet = convertR5TagsToStrings(right);
    if (!leftSet.equals(rightSet)) {
      return false;
    }
    return true;
  }
}
