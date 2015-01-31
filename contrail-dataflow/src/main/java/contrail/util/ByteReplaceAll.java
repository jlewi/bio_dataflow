package contrail.util;

import contrail.util.ByteUtil;

/**
 * Helper class for replacing all occurrences of one or more values in a byte array.
 *
 * This class trades memory for speed to do the replace as efficiently as possible.
 * We create a boolean array of size 256 corresponding to all possible values of a byte.
 * The value of each element specifies whether that element should be replaced.
 * Thus, the run time is O(n) where n is the length of the byte array.
 *
 * @author jeremy@lewi.us <Jeremy Lewi>
 *
 */
public class ByteReplaceAll {

	/**
	 * Store an array indicating whether each value is to be replaced.
	 */
	private boolean[] values_to_replace;

	// the value to replace the characters with.
	private byte new_value;

	// The encoding we use.
	public static final String encoding = "UTF-8";

	/**
	 * Constructor.
	 *
	 * @param match - A string representing the characters to be replaced.
	 * 	e.g "bcd" means bytes representing 'b' or 'c' or 'd' encoded in us ASCII should be replaced
	 *
	 * @param replace - The replacement character
	 */
	public ByteReplaceAll(String match, String replace){

		if (match.length() == 0){
			throw new RuntimeException("match must have length at least 1");
		}
		if (replace.length() != 1 ){
			throw new RuntimeException("replace should a length 1 string");
		}

		// Get the byte values
		byte[] replace_bytes = ByteUtil.stringToBytes(replace);

		if (replace_bytes.length != 1){
			throw new RuntimeException("The replacement character " + replace +
					" takes more than 1 byte to encode so it is not valid");
		}

		init (ByteUtil.stringToBytes(match),replace_bytes[0]);
	}

	/**
	 * Perform the actual initialization
	 */
	protected void init (byte[] match, byte replace){
		// initialize values_to_replace;
		values_to_replace = new boolean[256];
		java.util.Arrays.fill(values_to_replace, false);

		for (int index = 0; index < match.length; index++){
			int pos = ByteUtil.byteToUint(match[index]);
			values_to_replace[pos] = true;
		}
		new_value = replace;
	}

	/**
	 * Replace all bytes in the input whose value is in match with the
	 * value in replace.
	 *
	 * This is done in place to avoid an extra copy.
	 * @param input
	 * @return
	 */
	public void replaceAll(byte[] input){
		int pos;
		for (int index = 0; index < input.length; index++){
			// java represents bytes as signed integers in two's complement.
			// To convert to the corresponding value of a byte representing an unsigned
			// integer we add 255 to negative values.
			pos = ByteUtil.byteToUint(input[index]);
			if (values_to_replace[pos]){
				input[index] = new_value;
			}
		}
	}

}
