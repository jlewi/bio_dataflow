package contrail.sequences;

import java.util.Random;

public class AlphabetUtil {
  /**
   * Return a random sequence of characters of the specified length
   * using the given alphabet. This function is mostly used in unittests.
   *
   * @param length
   * @param alphabet
   * @return
   */
  public static String randomString(
      Random generator, int length, Alphabet alphabet) {
    // Generate a random sequence of the indicated length;
    char[] letters = new char[length];
    for (int pos = 0; pos < length; pos++) {
      // Randomly select the alphabet
      int rnd_int = generator.nextInt(alphabet.size());
      letters[pos] = alphabet.validChars()[rnd_int];
    }
    return String.valueOf(letters);
  }
}
