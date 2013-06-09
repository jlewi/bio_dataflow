package contrail.sequences;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import contrail.util.ByteUtil;

public class TestDNAAlphabetWithN {

  @Test
  public void testutf8ToInt() {
    Alphabet alphabet = DNAAlphabetWithNFactory.create();

    // Test that the UTF8 values for the letters in the alphabet are properly
    // mapped to their integer values.
    String strLetters = new String(alphabet.validChars());
    byte[] letters_bytes = ByteUtil.stringToBytes(strLetters);
    for (int pos = 0; pos < strLetters.length(); pos++) {
      byte bval = letters_bytes[pos];
      assertEquals(
          alphabet.utf8ToInt(bval), alphabet.letterToInt(
              strLetters.charAt(pos)));
    }
  }
}
