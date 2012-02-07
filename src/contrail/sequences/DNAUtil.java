package contrail.sequences;

import java.io.IOException;

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
  public static char canonicaldir(Sequence seq)
  {    
    Sequence complement = reverseComplement(seq);
    if (seq.compareTo(complement) <= 0)
    {
      return 'f';
    }
    return 'r';
  }
  
  /**
   *  
   * @param link
   * @return
   * @throws IOException
   */
  public static String flip_link(String link) throws IOException
  {
    if (link.equals("ff")) { return "rr"; }
    if (link.equals("fr")) { return "fr"; }
    if (link.equals("rf")) { return "rf"; }
    if (link.equals("rr")) { return "ff"; }

    throw new IOException("Unknown link type: " + link);
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
   * Convert the canonical representation of a sequence to the direction
   * given by the argument. The sequence is only copied if the direction
   * is "r"
   */
  public static Sequence canonicalToDir(Sequence seq, char dir) {
    if (dir == 'f') {
      return seq;
    }
    return reverseComplement(seq);
  }
}
