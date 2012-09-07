package contrail.sequences;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestDNAUtil {

  /**
   * Return a random sequence of characters of the specified length
   * using the given alphabet.
   *
   * @param length
   * @param alphabet
   * @return
   */
  public static char[] randomChars(int length, Alphabet alphabet) {
    // Generate a random sequence of the indicated length;
    char[] letters = new char[length];
    for (int pos = 0; pos < length; pos++) {
      // Randomly select the alphabet
      int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() -1));
      letters[pos] = alphabet.validChars()[rnd_int];
    }
    return letters;
  }


  @Test
  public void testReverseComplement() {

    final int MAX_LENGTH = 100;

    Alphabet alphabet = DNAAlphabetFactory.create();

    for (int length = 1; length < MAX_LENGTH; length++) {
      String str_seq = String.valueOf(randomChars(length, alphabet));

      Sequence seq = new Sequence(str_seq, alphabet);
      Sequence rc_seq = DNAUtil.reverseComplement(seq);
      assertEquals(rc_seq.size(), length);

      // Check the reverse complement
      for (int pos = 0; pos < length; pos++) {
        char forward = str_seq.charAt(pos);
        char reverse = rc_seq.at(length - pos -1);

        if (forward == 'A') {
          assertEquals(reverse, 'T');
        } else if (forward == 'T') {
          assertEquals(reverse, 'A');
        } else if (forward == 'G') {
          assertEquals(reverse, 'C');
        } else if (forward == 'C') {
          assertEquals(reverse, 'G');
        }
      }
    }
  }
  @Test
  public void testCanonicalDirCanonicalSeq() {

    final int MAX_LENGTH = 100;

    Alphabet alphabet = DNAAlphabetFactory.create();

    int ntrials = 100;
    for (int trial = 1; trial < ntrials; trial++) {
      int length = (int) Math.ceil(Math.random() * MAX_LENGTH);
      String str_seq = String.valueOf(randomChars(length, alphabet));
      Sequence seq = new Sequence(str_seq, alphabet);
      Sequence rc_seq = DNAUtil.reverseComplement(seq);
      String rc_str = rc_seq.toString();
      DNAStrand true_strand;
      if (seq.toString().compareTo(rc_seq.toString()) <= 0) {
        true_strand = DNAStrand.FORWARD;
        assertEquals(seq, DNAUtil.canonicalseq(seq));
      }
      else {
        true_strand = DNAStrand.REVERSE;
        assertEquals(rc_seq, DNAUtil.canonicalseq(seq));
      }
      assertEquals(DNAUtil.canonicaldir(seq), true_strand);
    }

    // Special case; string equals its reverse complement.
    Sequence seq = new Sequence("AT",alphabet);
    assertEquals(seq, DNAUtil.reverseComplement(seq));
    assertEquals(DNAUtil.canonicaldir(seq), DNAStrand.FORWARD);
    assertEquals(seq, DNAUtil.canonicalseq(seq));
  }

  @Test
  public void testIsPalindrom() {
    {
      Sequence sequence = new Sequence("AATT", DNAAlphabetFactory.create());
      assertTrue(DNAUtil.isPalindrome(sequence));
    }
    {
      Sequence sequence = new Sequence("AATTT", DNAAlphabetFactory.create());
      assertFalse(DNAUtil.isPalindrome(sequence));
    }
  }
}
