/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.sequences;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import contrail.util.ByteUtil;

public class TestSequence {
  final int BITSPERITEM = 32;

  public static int determineLCM(int larger, int smaller) {

    if (smaller > larger) {
      int tmp;
      tmp = smaller;
      smaller = larger;
      larger = tmp;
    }

    int lcm = larger;

    while ((lcm % smaller) != 0) {
      lcm += larger;
    }
    return lcm;
  }

  /**
   * Return a random sequence of characters of the specified length using the
   * given alphabet.
   * 
   * @param length
   * @param alphabet
   * @return
   */
  public static String randomChars(int length, Alphabet alphabet) {
    // Generate a random sequence of the indicated length;
    char[] letters = new char[length];
    for (int pos = 0; pos < length; pos++) {
      // Randomly select the alphabet
      int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() - 1));
      letters[pos] = alphabet.validChars()[rnd_int];
    }
    return String.valueOf(letters);
  }

  @Test
  public void testDNASequence() {
    Alphabet alphabet = DNAAlphabetFactory.create();
    // Run some unittests to check that Sequence is working.
    // TODO (jeremy@lewi.us): How can we test it works for other alpahbets.
    //
    // We want to check sequences of length 1 to NLCM where
    // NLCM  = LCM(alphabet.bitsPerLetter, 8)/alphabet.bitsPerLetter
    // where LCM stands for least common multiple. This should cover all possible
    // cases for how much padding/overlap.

    int bits_lcm = determineLCM(BITSPERITEM, alphabet.bitsPerLetter()); // DNASequence uses

    int max_length = (int) Math.ceil((double) bits_lcm
        / alphabet.bitsPerLetter());

    for (int length = 1; length < max_length; length++) {
      // Generate a random sequence of the indicated length;
      char[] letters = new char[length];
      for (int pos = 0; pos < length; pos++) {
        // Randomly select the alphabet
        int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() - 1));
        letters[pos] = alphabet.validChars()[rnd_int];
      }

      // Create the sequence
      Sequence sequence = new Sequence(letters, alphabet);

      assertEquals(sequence.size(), letters.length);

      // Check the characters match.
      for (int pos = 0; pos < length; pos++) {
        assertEquals(sequence.at(pos), letters[pos]);
      }

      // Check its padded.
      for (int pos = length; pos < sequence.capacity(); pos++) {
        assertEquals(sequence.at(pos), alphabet.EOS());
      }
    }
  }

  @Test
  public void testReadCharSequence() {
    Alphabet alphabet = DNAAlphabetFactory.create();
    // Create the sequence
    Sequence sequence = new Sequence(alphabet);

    // Run some unittests to check that Sequence is working.
    // TODO (jeremy@lewi.us): How can we test it works for other alpahbets.
    //
    // We want to check sequences of length 1 to NLCM where
    // NLCM  = LCM(alphabet.bitsPerLetter, 8)/alphabet.bitsPerLetter
    // where LCM stands for least common multiple. This should cover all possible
    // cases for how much padding/overlap.

    int bits_lcm = determineLCM(BITSPERITEM, alphabet.bitsPerLetter()); // DNASequence uses

    int max_length = (int) Math.ceil((double) bits_lcm
        / alphabet.bitsPerLetter());

    // We iterate over the lengths in reverse order because we want to make sure when reading in a shorter
    // string we properly zero out any unset bits.
    for (int length = max_length; length >= 1; length--) {
      // Generate a random sequence of the indicated length;
      String letters = randomChars(length, alphabet);

      sequence.readCharSequence(letters);
      assertEquals(sequence.size(), letters.length());

      // Check the characters match.
      for (int pos = 0; pos < length; pos++) {
        assertEquals(sequence.at(pos), letters.charAt(pos));
      }

      // Check its padded.
      for (int pos = length; pos < sequence.capacity(); pos++) {
        assertEquals(sequence.at(pos), alphabet.EOS());
      }
    }
  }

  @Test
  public void testDNASequenceReadPackedBytes() {
    // Test readBytes() and toBytes

    Alphabet alphabet = DNAAlphabetFactory.create();
    // Run some unittests to check that Sequence is working.
    // TODO (jeremy@lewi.us): How can we test it works for other alpahbets.
    //
    // We want to check sequences of length 1 to NLCM where
    // NLCM  = LCM(alphabet.bitsPerLetter, 8)/alphabet.bitsPerLetter
    // where LCM stands for least common multiple. This should cover all possible
    // cases for how much padding/overlap there is.

    int bits_lcm = determineLCM(BITSPERITEM, alphabet.bitsPerLetter()); // DNASequence uses

    int max_length = (int) Math.ceil((double) bits_lcm
        / alphabet.bitsPerLetter());

    for (int length = 1; length < max_length; length++) {
      // Generate a random sequence of the indicated length;
      char[] letters = new char[length];
      for (int pos = 0; pos < length; pos++) {
        // Randomly select the letters
        int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() - 1));
        letters[pos] = alphabet.validChars()[rnd_int];

      }

      // Create the sequence
      Sequence true_sequence = new Sequence(letters, alphabet);
      byte[] bytes = true_sequence.toPackedBytes();

      // reduce bytes to the minimum number of bytes
      int num_bytes = (int) Math
          .ceil((alphabet.bitsPerLetter() * length) / 8.0);
      byte[] min_bytes = new byte[num_bytes];
      for (int pos = 0; pos < num_bytes; pos++) {
        min_bytes[pos] = bytes[pos];
      }

      // Read the sequence back in and check it matches
      Sequence sequence = new Sequence(alphabet);
      sequence.readPackedBytes(min_bytes, length);

      assertEquals(sequence.size(), letters.length);

      // Check the characters match.
      for (int pos = 0; pos < length; pos++) {
        assertEquals(sequence.at(pos), letters[pos]);
      }

      // Check its padded.
      for (int pos = length; pos < sequence.capacity(); pos++) {
        assertEquals(sequence.at(pos), alphabet.EOS());
      }
    }
  }

  @Test
  public void testcomputeEditDistance() {
    {
      Alphabet alphabet = DNAAlphabetFactory.create();
      Sequence s1 = new Sequence("AATG", alphabet);
      Sequence s2 = new Sequence("AATG", alphabet);

      int retval = s1.computeEditDistance(s2);
      int expected_val = new Integer(0);
      assertEquals(retval, expected_val);
    }
    {
      Alphabet alphabet = DNAAlphabetFactory.create();
      Sequence s1 = new Sequence("AAT", alphabet);
      Sequence s2 = new Sequence("ACGC", alphabet);

      int retval = s1.computeEditDistance(s2);
      int expected_val = new Integer(3);
      assertEquals(retval, expected_val);
    }
    {
      Alphabet alphabet = DNAAlphabetFactory.create();
      Sequence s1 = new Sequence("ACGC", alphabet);
      Sequence s2 = new Sequence("CGC", alphabet);

      int retval = s1.computeEditDistance(s2);
      int expected_val = new Integer(1);
      assertEquals(retval, expected_val);
    }
  }

  @Test
  public void testDNASequenceReadUTF8() {

    Alphabet alphabet = DNAAlphabetFactory.create();
    // Run some unittests to check that Sequence can correctly read a sequence
    // of bytes in UTF8.
    // TODO (jeremy@lewi.us): How can we test it works for other alpahbets.
    //
    // We want to check sequences of length 1 to NLCM where
    // NLCM  = LCM(alphabet.bitsPerLetter, 8)/alphabet.bitsPerLetter
    // where LCM stands for least common multiple. This should cover all possible
    // cases for how much padding/overlap.
    int bits_lcm = determineLCM(BITSPERITEM, alphabet.bitsPerLetter()); // DNASequence uses

    int max_length = (int) Math.ceil((double) bits_lcm
        / alphabet.bitsPerLetter());

    for (int length = 1; length < max_length; length++) {
      // Generate a random sequence of the indicated length;
      char[] letters = new char[length];
      for (int pos = 0; pos < length; pos++) {
        // Randomly select the alphabet
        int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() - 1));
        letters[pos] = alphabet.validChars()[rnd_int];
      }
      // Create a sequence of bytes encoding the sequence in UTF8
      byte[] utf8_letters = ByteUtil.stringToBytes(new String(letters));

      Sequence sequence = new Sequence(alphabet);
      sequence.readUTF8(utf8_letters);

      assertEquals(sequence.size(), length);
      // Check the characters match.
      for (int pos = 0; pos < length; pos++) {
        assertEquals(sequence.at(pos), letters[pos]);
      }

      // Check its padded.
      for (int pos = length; pos < sequence.capacity(); pos++) {
        assertEquals(sequence.at(pos), alphabet.EOS());
      }

      // Check readUTF8 works when the buffer is longer than the sequence.
      byte[] utf8_longer = Arrays.copyOf(utf8_letters, length * 3);
      sequence = new Sequence(alphabet);
      sequence.readUTF8(utf8_longer, length);
      assertEquals(sequence.size(), length);
      for (int pos = 0; pos < length; pos++) {
        assertEquals(sequence.at(pos), letters[pos]);
      }
    }
  }

  @Test
  public void testDNAsequenceSetAt() {
    Alphabet alphabet = DNAAlphabetFactory.create();
    // test that setAt works for a sequence of characters
    // We want to check sequences of length 1 to NLCM where
    // NLCM  = LCM(alphabet.bitsPerLetter, 8)/alphabet.bitsPerLetter
    // where LCM stands for least common multiple. This should cover all possible
    // cases for how much padding/overlap.
    int bits_lcm = determineLCM(BITSPERITEM, alphabet.bitsPerLetter()); // DNASequence uses

    if (bits_lcm <= 32) {
      bits_lcm = 96;
    }
    int max_length = (int) Math.ceil((double) bits_lcm
        / alphabet.bitsPerLetter());

    // Generate a random sequence of the indicated length;
    char[] letters = new char[max_length];
    for (int pos = 0; pos < max_length; pos++) {
      // Randomly select the alphabet
      int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() - 1));
      letters[pos] = alphabet.validChars()[rnd_int];
    }

    Sequence sequence = new Sequence(letters, alphabet);

    for (int start = 0; start < max_length; start++) {
      for (int end = start + 1; end <= max_length; end++) {
        int[] letters_ints = new int[end - start];
        for (int pos = 0; pos < letters_ints.length; pos++) {
          // Randomly select the alphabet
          int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() - 1));
          letters_ints[pos] = rnd_int;
        }

        sequence.setAt(start, letters_ints);

        for (int pos = start; pos < end; pos++) {
          assertEquals(sequence.valAt(pos), letters_ints[pos - start]);
        }
      }
    }
  }

  @Test
  public void testDNAsubsequence() {
    Alphabet alphabet = DNAAlphabetFactory.create();
    // Run some unittests to check that Sequence can correctly return
    // a subsequence.
    //
    // We want to check sequences of length 1 to NLCM where
    // NLCM  = LCM(alphabet.bitsPerLetter, 8)/alphabet.bitsPerLetter
    // where LCM stands for least common multiple. This should cover all possible
    // cases for how much padding/overlap.
    int bits_lcm = determineLCM(BITSPERITEM, alphabet.bitsPerLetter()); // DNASequence uses

    if (bits_lcm <= 32) {
      bits_lcm = 96;
    }
    int max_length = (int) Math.ceil((double) bits_lcm
        / alphabet.bitsPerLetter());

    // Generate a random sequence of the indicated length;
    String letters = randomChars(max_length, alphabet);

    Sequence sequence = new Sequence(letters, alphabet);
    for (int start = 0; start < max_length; start++) {
      for (int end = start + 1; end <= max_length; end++) {
        Sequence subsequence = sequence.subSequence(start, end);
        assertEquals(subsequence.size(), end - start);
        for (int index = 0; index < subsequence.size(); index++) {
          assertEquals(subsequence.at(index), sequence.at(start + index));

          // Make sure all unset bits in the last integer are zero.
          byte[] bytes = subsequence.toPackedBytes();
          int[] buffer = ByteUtil.bytesToInt(bytes);
          long num_unset = buffer.length * 32 - subsequence.size()
              * alphabet.bitsPerLetter();
          assertTrue(num_unset < 32);
          if (num_unset > 0) {
            int val_unset = buffer[buffer.length - 1] >>> (32 - num_unset);
            assertEquals(val_unset, 0);
          }
        }
      }
    }
  }

  @Test
  public void testtoString() {
    Alphabet alphabet = DNAAlphabetFactory.create();
    // Run some tests to verify toString() works.

    int ntrials = 10;
    int MAX_LENGTH = 200;
    for (int trial = 0; trial < ntrials; trial++) {
      // Determine the sequence length.
      int rnd_length = (int) Math.ceil(Math.random() * MAX_LENGTH);

      String letters = randomChars(rnd_length, alphabet);
      Sequence sequence = new Sequence(letters, alphabet);

      String seq_string = sequence.toString();
      assertEquals(seq_string, new String(letters));
    }
  }

  @Test
  public void testadd() {
    Alphabet alphabet = DNAAlphabetFactory.create();
    // Run some tests to verify toString() works.

    // We want to check all pairs of length (i,j) where i,j range from 1 to NLCM where
    // NLCM  = LCM(alphabet.bitsPerLetter, 8)/alphabet.bitsPerLetter
    // where LCM stands for least common multiple. This should cover all possible
    // cases for how much padding/overlap there is.
    int bits_lcm = determineLCM(BITSPERITEM, alphabet.bitsPerLetter()); // DNASequence uses

    if (bits_lcm <= 32) {
      bits_lcm = 96;
    }

    final int max_length = (int) Math.ceil((double) bits_lcm
        / alphabet.bitsPerLetter());

    for (int src_length = 1; src_length < max_length; src_length++) {
      String src_letters = randomChars(src_length, alphabet);

      for (int dest_length = 1; dest_length < max_length; dest_length++) {
        String dest_letters = randomChars(dest_length, alphabet);

        //src_letters = "CCCGGGGCGCGACCGG";
        //dest_letters = "GC";
        Sequence src_sequence = new Sequence(src_letters, alphabet);
        Sequence dest_sequence = new Sequence(dest_letters, alphabet);

        src_sequence.add(dest_sequence);
        String true_sum = String.valueOf(src_letters)
            + String.valueOf(dest_letters);

        String sum_str = src_sequence.toString();
        assertEquals(src_sequence.size(), src_length + dest_length);
        assertEquals(true_sum, sum_str);
      }
    }

    // Test addition with zero length sequences.
    for (int length = 1; length < max_length; length++) {
      String letters = randomChars(length, alphabet);
      int capacity = (int) Math.ceil(Math.random() * max_length);
      {
        Sequence sequence = new Sequence(letters, alphabet);
        Sequence zero_seq = new Sequence(alphabet, capacity);

        // Add the zero length sequence to sequence.
        sequence.add(zero_seq);
        assertEquals(letters, sequence.toString());
        assertEquals(sequence.size(), letters.length());
      }
      // Do the addition the other way.
      {
        Sequence sequence = new Sequence(letters, alphabet);
        Sequence zero_seq = new Sequence(alphabet, capacity);

        // Add the zero length sequence to sequence.
        zero_seq.add(sequence);
        assertEquals(letters, zero_seq.toString());
        assertEquals(zero_seq.size(), letters.length());
      }
    }
  }

  @Test
  public void testAddUnzeroed() {
    // This function verifies that we properly zero out the data
    // before doing the add. We do this as follows
    // We create a byte that is filled with 1's. We then create a sequence
    // of length from this byte buffer. We then add another sequence of length
    // 1 to this character. If the bits aren't zeroed out correctly
    // the result won't be correct.
    byte[] input_bytes = new byte[1];
    input_bytes[0] = 0xFFFFFFFF;
    Sequence src = new Sequence(DNAAlphabetFactory.create(), 1);
    src.readPackedBytes(input_bytes, 1);

    //new Sequence(, DNAAlphabetFactory.create());
    Alphabet alphabet = DNAAlphabetFactory.create();
    Sequence dest = new Sequence(alphabet, 1);
    dest.setAt(0, 1);
    dest.setSize(1);

    Sequence expected_sum = new Sequence("TC", DNAAlphabetFactory.create());

    src.add(dest);
    assertEquals(expected_sum, src);
  }

  @Test
  public void testCompare() {
    int MAX_LENGTH = 130;
    Alphabet alphabet = DNAAlphabetFactory.create();
    for (int shorter = 1; shorter < MAX_LENGTH; shorter++) {
      for (int longer = shorter; longer < MAX_LENGTH; longer++) {
        String shorter_str = randomChars(shorter, alphabet);
        String longer_str = randomChars(longer, alphabet);
        Sequence shorter_seq = new Sequence(shorter_str, alphabet);
        Sequence longer_seq = new Sequence(longer_str, alphabet);
        int seq_val = shorter_seq.compareTo(longer_seq);
        int str_val = shorter_str.compareTo(longer_str);

        // Check the signs match.
        str_val = str_val > 0 ? 1 : str_val;
        str_val = str_val < 0 ? -1 : str_val;
        assertEquals(shorter_seq.compareTo(longer_seq), str_val);

        str_val = longer_str.compareTo(shorter_str);
        str_val = str_val > 0 ? 1 : str_val;
        str_val = str_val < 0 ? -1 : str_val;
        assertEquals(longer_seq.compareTo(shorter_seq), str_val);
      }
    }
  }

  @Test
  public void testCompressedSequence() {
    Sequence sequence = new Sequence("ACTGT", DNAAlphabetFactory.create());
    CompressedSequence compressed = sequence.toCompressedSequence();
    assertEquals(compressed.getLength().intValue(), sequence.size());

    Sequence reconstructed = new Sequence(DNAAlphabetFactory.create());
    reconstructed.readCompressedSequence(compressed);
    assertEquals(sequence, reconstructed);
  }

  @Test
  public void testBase64() {
    // One potential problem is that extra bits aren't correctly zero'd out.
    // To try to catch this we create the sequence outside the loop and
    // go from long to short sequences. This won't produce correct results
    // unless extra bits are correctly zero'd out.
    //Sequence sequence = new Sequence(DNAAlphabetFactory.create());
    for (int length = 100; length >= 0; --length) {
      String letters = randomChars(length, DNAAlphabetFactory.create());
      Sequence sequence = new Sequence(letters, DNAAlphabetFactory.create());
      String base64 = sequence.toBase64();

      byte[] expectedBytes = Arrays.copyOf(sequence.toPackedBytes(),
          sequence.numPackedBytes());
      byte[] decodedBytes = Base64.decodeBase64(base64);
      assertTrue(Arrays.equals(expectedBytes, decodedBytes));

      Sequence reconstructed = new Sequence(DNAAlphabetFactory.create());
      reconstructed.readBase64(base64, length);
      assertEquals(sequence, reconstructed);
    }
  }
}
