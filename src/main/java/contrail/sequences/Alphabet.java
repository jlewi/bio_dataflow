package contrail.sequences;

/**
 * Base class for alphabets.
 *
 * @author jlewi
 *
 */
public abstract class Alphabet {

  public abstract int bitsPerLetter ();

  /**
   * Return a byte mask which would return a single letter encoded the least
   * significant bits_per_letter bits.
   * @return
   */
  public abstract int letterMask();

  /**
   * Convert an int value to the correct letter.
   * We do no error checking. You will get an out of bounds exception if the value
   * corresponds to a value greater than the number of characters in the alphabet.
   */
  public abstract char intToLetter(int val);

  /**
   * Convert a letter into the corresponding int value.
   * We do no error checking. You will get an exception if the letter isn't in the alphabet.
   * Case matters.
   */
  public abstract int letterToInt(char letter);

  /**
   * Convert a letter into the corresponding int value.
   * We do no error checking. You will get an exception if the letter isn't in the alphabet.
   * Case matters.
   */
  public int letterToInt(String letter) {
    return letterToInt(letter.charAt(0));
  }

  /**
   * Size of the alphabet. Includes the end of sequence character.
   */
  public abstract int size();

  /**
   * The set of valid characters in the alphabet (i.e non end of sequence characters);
   */
  public abstract char[] validChars();

  /**
   * Check if the indicated character is in the set of valid chars.
   */
  public boolean hasLetter (char letter) {
    char [] letters = this.validChars();
    for (int index = 0; index < letters.length; index++){
      if (letters[index] == letter) {
        return true;
      }
    }
    return false;
  }
  /**
   * Return the end of sequence character
   */
  public abstract char EOS ();

  /**
   * Return the int value of the end of sequence character
   */
  public abstract int intEOS();

  /**
   * Whether this alphabet has a unique EOS character.
   */
  public abstract boolean hasEOS();

  /**
   * Return the number of bits in each item into which we pack the letters.
   * Do not get this confused with bitsPerLetter().
   */
  public int bitsPerItem() {
    // We use an integer array so the letters get packed into 32 bit items.
    return 32;
  }

  /**
   * Convert a byte encoding a UTF-8 character, into the appropriate value.
   *
   * TODO (jlewi): Add a default implementation which converts the byte
   * to a string and then from a string to a letter.
   */
  public abstract int utf8ToInt(byte bval);

  /**
   * Convert an array of bytes encoding a UTF-8 character, into an array
   * of ints of the appropriate value.
   *
   * TODO (jlewi): Add a default implementation which converts the byte
   * to a string and then from a string to a letter.
   */
  public abstract int[] utf8ToInt(byte[] bval);

  /**
   * Convert an array of bytes encoding a UTF-8 character, into an array
   * of ints of the appropriate value.
   *
   * TODO (jlewi): Add a default implementation which converts the byte
   * to a string and then from a string to a letter.
   *
   * @param bval: The array of bytes to convert.
   * @param length: The number of bytes in bval to use.
   * @return
   */
  public abstract int[] utf8ToInt(byte[] bval, int length);

  /**
   * Check if two alphabets are equal
   */
  public boolean equals(Alphabet other) {
    Class otherType = other.getClass();

    return otherType.isInstance(this);
  }
}

