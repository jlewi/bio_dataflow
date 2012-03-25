package contrail.sequences;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import contrail.util.ByteUtil;

public class TestDNAAlphabet {

  @Test
  public void testutf8ToInt() {
    
    Alphabet alphabet = DNAAlphabetFactory.create();

    // Test that the UTF8 values for the letters in the alphabet are properly mapped
    // to their integer values.
    String str_letters = new String(alphabet.validChars());
    byte[] letters_bytes = ByteUtil.stringToBytes(str_letters);
    for (int pos = 0; pos < str_letters.length(); pos++) {
      byte bval = letters_bytes[pos];
      assertEquals(alphabet.utf8ToInt(bval), alphabet.letterToInt(str_letters.charAt(pos)));
    }
  }  
}
