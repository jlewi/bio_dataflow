package contrail.sequences;

import contrail.util.ByteUtil;

/**
 * Factory to create the DNAAlphabet. We use a factory because we only want
 * one instance of the alphabet but we need to initialize it the first time its
 * called.
 *
 * The resulting Alphabet represents the 4 DNA bases {"A", "C", "G", "T"}
 * using 2 bits per base; i.e. each base is assigned a number [0, 3].
 */
public class DNAAlphabetFactory {

	// The instance of the alphabet.
	private static Alphabet alphabet = null;

	// maximum unsigned integer that can be represented using an int.
	// if we go larger than this than we need to use a long and conver that long into the equivalent
	// signed representation using an int.
	protected final static int MAXUINT = java.lang.Integer.MAX_VALUE;


	/**
	 * Defines the alphabet for encoding DNA sequences.
	 * @author jlewi
	 *
	 * TODO (jlewi): Should we use a factory to get the alphabet class, so that the factory
	 * can make sure the class is already initialized?
	 */
	private static class DNAAlphabet extends Alphabet {

		/**
		 * The DNAAlphabet doesn't actually use an end of sequence character to indicate
		 * the end of the sequence; Rather we store the length of the sequence separatly.
		 * For compatibility with the API we still need to define the EOS_CHAR.
		 */
		private final char EOS_CHAR  = 'A';
		private final char[] ALPHABET = {'A', 'C', 'G', 'T'};

		// The non end of sequence characters.
		private final char[] VALID_ALPHABET = {'A', 'C', 'G', 'T'};

		private int bits_per_letter;

		private int eos_int;
		/**
		 * Create a bit mask that we can use to get the bits
		 * in a int encoding the values we are interested in.
		 */
		private int letter_mask;
		private java.util.HashMap<java.lang.Character, java.lang.Integer> letters_to_num;

		// A an array such that ut8_map[b] = the int value
		// assigned to character b where b is the utf8 value
		// of one of the characters in the alphabet.
		private int[] utf8_map;

		public DNAAlphabet() {
			if (ALPHABET.length > MAXUINT) {
				throw new RuntimeException("Alphabet length exceeds maximum length");
			}

			letters_to_num = new java.util.HashMap<Character, java.lang.Integer>();
			for (int pos = 0; pos < ALPHABET.length; pos++) {
				// pos is guaranteed to be less than MAXUINT so the unisgned representation
				// is the same as for signed integer.
				letters_to_num.put(ALPHABET[pos], pos);
			}

			bits_per_letter = (int)Math.ceil(Math.log(ALPHABET.length)/Math.log(2));

			letter_mask = 0x0;

			int bit = 0x1;
			for (int pos = 0; pos < bitsPerLetter(); pos++) {
				letter_mask = (letter_mask | bit);
				bit = (bit << 1);
			}

			// Initialize the utf8_map.
			utf8_map = new int[256];
			for (int pos = 0; pos < utf8_map.length; pos++) {
				utf8_map[pos] = letterToInt(EOS_CHAR);
			}

			for (int index = 0; index < VALID_ALPHABET.length; index++){
				// Convert this character to a byte using utf8.
				String str_rep = new String(new char[]{VALID_ALPHABET[index]});
				byte[] utf8_vals = ByteUtil.stringToBytes(str_rep);
				if (utf8_vals.length != 1){
					throw new RuntimeException ("length of utf8_vals should be 1.");
				}
				utf8_map[ByteUtil.byteToUint(utf8_vals[0])] = letterToInt(VALID_ALPHABET[index]);
			}

			eos_int = letterToInt(EOS_CHAR);
		}

		public int bitsPerLetter () {
			return bits_per_letter;
		}

		/**
		 * Return a byte mask which would return a single letter encoded the least
		 * significant bits_per_letter bits.
		 * @return
		 */
		public int letterMask(){
			return letter_mask;
		}

		/**
		 * Convert a byte value to the correct letter.
		 * We do no error checking. You will get an out of bounds exception if the value
		 * corresponds to a value greater than the number of characters in the alphabet.
		 */
		public char intToLetter(int val){
			// val is assumed to be an integer < MAXUINT so its unsigned value is the same
			// as the signed representation so we don't need to do any conversion.
			return ALPHABET[val];
		}

		/**
		 * Convert a letter into the specified int.
		 * We do no error checking. You will get an exception if the letter isn't in the alphabet.
		 * Case matters.
		 */
		public int letterToInt(char letter){
			return letters_to_num.get(letter);
		}

		/**
		 * Size of the alphabet. Includes the end of sequence character.
		 */
		public int size() {
			return ALPHABET.length;
		}

		/**
		 * The set of valid characters in the alphabet (i.e non end of sequence characters);
		 */
		public final char[] validChars() {
			return VALID_ALPHABET;
		}

		/**
		 * Return the end of sequence character
		 */
		public final char EOS () {
			return EOS_CHAR;
		}
		/**
		 * Return the int value of the end of sequence character
		 */
		public int intEOS() {
			return eos_int;
		}

		/**
		 * Convert a byte encoding a UTF-8 character, into the appropriate value.
		 *
		 */
		public int utf8ToInt(byte bval) {
			return utf8_map[ByteUtil.byteToUint(bval)];
		}

		/**
		 * Convert an array of bytes encoding a UTF-8 character, into an array
		 *  of ints of the appropriate value.
		 */
    public int[] utf8ToInt(byte[] bval) {
      return utf8ToInt(bval, bval.length);
    }

    /**
     * @param bval: The byte buffer.
     * @param length: How many bytes in in bval to convert.
     */
    public int[] utf8ToInt(byte[] bval, int length) {
      int[] nums = new int[length];
      ByteUtil.byteToUint(bval, nums, length);
      for (int i = 0; i < length; i++) {
        nums[i] = utf8_map[nums[i]];
      }
      return nums;
    }

	  public boolean hasEOS() {
	    return false;
	  }
	}

	/**
	 * Create the alphabet if it doesn't exist or return the existing copy if it does.
	 */
	public static Alphabet create() {
		if (alphabet == null){
			alphabet = new DNAAlphabetFactory.DNAAlphabet();
		}
		return alphabet;
	}
}