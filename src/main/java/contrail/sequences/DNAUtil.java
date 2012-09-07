package contrail.sequences;

/**
 * A set of routines for manipulating DNA sequences.
 *
 * @author jlewi
 *
 */
public class DNAUtil {

  /**
   * Compute the reverse complement of the DNA sequence.
   *
   * TODO(jlewi): Add a unittest.
   * TODO(jlewi): We could probably speed this up by precomputing the integer
   *     values for the complement so that we don't have to convert the characters
   *     to from the integer values.
   * @param seq
   * @return
   */
  public static Sequence reverseComplement(Sequence seq)
  {
    Sequence complement = new Sequence(seq.getAlphabet(), seq.capacity());

    for (int i = seq.size() - 1; i >= 0; i--)
    {
      int write = seq.size() - 1 - i;
      if      (seq.at(i) == 'A') { complement.setAt(write, 'T'); }
      else if (seq.at(i) == 'T') { complement.setAt(write, 'A'); }
      else if (seq.at(i) == 'C') { complement.setAt(write, 'G'); }
      else if (seq.at(i) == 'G') { complement.setAt(write, 'C'); }
    }

    complement.setSize(seq.size());
    return complement;
  }

  /**
   * Compare a string to its reverse complement.
   *
   * @param seq - The sequence.
   * @return - 'f' if the DNA sequence precedes is reverse complement
   *  lexicographically (based on the integer values of the sequence). 'r' otherwise.
   */
  public static DNAStrand canonicaldir(Sequence seq)
  {
    Sequence complement = reverseComplement(seq);
    if (seq.compareTo(complement) <= 0)
    {
      return DNAStrand.FORWARD;
    }
    return DNAStrand.REVERSE;
  }

  /**
   * Returns the canonical version of a DNA sequence.
   *
   * The canonical version of a sequence is the result
   * of comparing a DNA sequence to its reverse complement
   * and returning the one which comes first when ordered lexicographically.
   *
   * @param seq
   * @return - The canonical version of the DNA sequence.
   */
  public static Sequence canonicalseq(Sequence seq)
  {
    Sequence rc = reverseComplement(seq);
    if (seq.compareTo(rc) < 0)
    {
      return seq;
    }

    return rc;
  }

  /**
   * Convert the sequence to the relative direction given by DNAStrand.
   * The sequence is only copied if the strand is reverse.
   * @param seq: The sequence.
   * @return: If strand is FORWARD the sequence is returned otherwise
   *   its reverse complement is returned.
   */
  public static Sequence sequenceToDir(Sequence seq, DNAStrand strand) {
    if (strand == DNAStrand.FORWARD) {
      return seq;
    }
    return reverseComplement(seq);
  }

  /**
   * Merge two overlapping sequences. The inputs are modified.
   * @param src: This sequence is potentially modified by the merge.
   * @param dest: The second overlapping sequence
   * @param overlap: The amount of overlap
   * @return: A reference to the merged sequence.
   */
  public static Sequence mergeSequences(
      Sequence src, Sequence dest, int overlap) {
    // TODO(jlewi): Add a unittest.
    // TODO(jlewi): Might be more efficient not to check the overlap.

    Sequence src_overlap = new Sequence(src);
    src = src.subSequence(src.size() - overlap, src.size());
    Sequence dest_overlap = new Sequence(dest);
    dest = dest.subSequence(0, overlap);

    if (!src_overlap.equals(dest_overlap)) {
      throw new RuntimeException(
          "Source (" + src_overlap.toString() + ") != dest (" +
          dest_overlap.toString() + ")");
    }

    dest.subSequence(overlap, dest.size());
    src.add(dest);
    return src;
  }

  /**
   * A sequence is a palindrome if it equals its reverse complement.
   *
   * @param seq
   * @return: True iff the sequence is a palindrome.
   */
  public static boolean isPalindrome(Sequence seq) {
    if (seq.getAlphabet() == DNAAlphabetFactory.create()) {
      if ((seq.size() & 0x1) == 1) {
        // Odd length sequences can't be palindromes for the DNA alphabet.
        return false;
      }
    }
    if (seq.equals(reverseComplement(seq))) {
      return true;
    }
    return false;
  }
}
