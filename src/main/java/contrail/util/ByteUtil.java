package contrail.util;

/**
 * Some simple utility functions for working with byte arrays.
 *
 * @author jeremy@lewi.us <Jeremy Lewi>
 *
 */
public class ByteUtil {

	/**
	 * Convert bytes to string; turn exceptions into runtime exceptions.
	 */
	public static String bytesToString(byte[] byte_data, String encoding){
		String out_string = "";
		try {
			out_string = new String(byte_data, encoding);
		}
		catch (java.io.UnsupportedEncodingException exception) {
			throw new RuntimeException("There was problem encoding the bytes as a string");
		}
		return out_string;
	}

	/**
	 * Check whether a sequence of bytes representing a UTF-8 string contains
	 * any multibyte characters.
	 *
	 */
	public static boolean hasMultiByteChars(byte[] data) {
		// In UTF8 a multi byte character has a 1 in the leading bit.
		for (int index = 0; index < data.length; index++){
			if ((data[index] & 0x80) == 0x80) {
				return true;
			}
		}
		return false;
	}


	 /**
   * Replace multibyte characters with the specified value.
   * For efficiency the replacement happens in place.
   * The result is stored in the first n elements of the input
   * where n is the value returned by the function.
   *
   * @param data - The array of bytes in which to replace multi-byte characters.
   * @param new_value - The value to replace multi-byte characters with.
   * @param length - Only consider the bytes up to position data[length-1]
   * @return - The number of bytes in the resulting array after replacing
   *           multi-byte characters with new_value.
   */
  public static int replaceMultiByteChars(byte[] data, byte new_value, int length){
    if ( (new_value & 0x80) == 0x80) {
      throw new RuntimeException("The new value doesn't correspond to a single byte character in UTF-8");
    }
    // Keep track of the indexes corresponding to
    // where we should read/write the next bytes.
    int read=0;
    int write=0;

    while (read < length){
      if ((data[read] & 0x80) == 0x80){
        // This is a multi-byte character
        // so write the replacement value.
        data[write] = new_value;
        read++;
        write++;

        // Keep incrementing read until we get to a character
        // that doesn't start with 10 in binary. This
        // will be the start of a new sequence.
        while ((data[read] & 0xc0) == 0x80 ){
          read++;
          if (read == length){
            break;
          }
        }
      }
      else{
        // This is not a multi-byte character so write it.
        data[write]=data[read];
        write++;
        read++;
      }
    }

    return write;
  }

	/**
	 * Replace multibyte characters with the specified value.
	 * For efficiency the replacement happens in place.
	 * The result is stored in the first n elements of the input
	 * where n is the value returned by the function.
	 *
	 * @param data - The array of bytes in which to replace multi-byte characters.
	 * @param new_value - The value to replace multi-byte characters with.
	 * @return - The number of bytes in the resulting array after replacing
	 *           multi-byte characters with new_value.
	 */
	public static int replaceMultiByteChars(byte[] data, byte new_value){
		return replaceMultiByteChars(data, new_value, data.length);
	}

	/**
	 * Convert a string in UTF-8 to bytes using a single byte per character.
	 * The conversion ensures that each character
	 * can be encoded using a single byte. If the string contains characters requiring multiple
	 * bytes we throw an exception.
	 */
	public static byte[] stringToBytes(String text){

		byte[] bytes;
		try {
			bytes = text.getBytes("UTF-8");
		}
		catch (java.io.UnsupportedEncodingException exception) {
			throw new RuntimeException("There was problem getting the byte encoding for the input.");
		}

		if (hasMultiByteChars(bytes)) {
			throw new RuntimeException("Some of the characters in the input string cannot be encoded using a single byte.");
		}
		return bytes;
	}

	/**
	 * Convert a byte to the equivalent value an unsigned integer.
	 *
	 * java represents bytes as signed integers in two's complement.
   * To convert to the corresponding value of a byte representing an unsigned
   * integer we add 255 to negative values.
   *
	 */
	public static int byteToUint(byte val){
	  int num;
	  num = val <0 ? 256 + val : val;
	  return num;
	}

	/**
   * Convert an array of bytes to the equivalent value an unsigned integer.
   *
   * java represents bytes as signed integers in two's complement.
   * To convert to the corresponding value of a byte representing an unsigned
   * integer we add 255 to negative values.
   *
   * @param bytes - The bytes to convert
   * @param uints - the buffer of uints to write to.
   *
   * TODO(jeremy@lewi.us): Add a unittest.
   */
  public static void byteToUint(byte[] val, int[] uints){
    byteToUint(val, uints, val.length);
  }

  /**
   * Convert an array of bytes to the equivalent value an unsigned integer.
   *
   * java represents bytes as signed integers in two's complement.
   * To convert to the corresponding value of a byte representing an unsigned
   * integer we add 255 to negative values.
   *
   * @param bytes - The bytes to convert
   * @param uints - the buffer of uints to write to.
   * @param length: Only the first length values are copied from val to uints.
   * TODO(jeremy@lewi.us): Add a unittest.
   */
  public static void byteToUint(byte[] val, int[] uints, int length){
    for (int pos = 0; pos < length; pos++) {
      uints[pos] = val[pos] <0 ? 256 + val[pos] : val[pos];
    }
  }

  /**
   * Convert the value of an unsigned integer to the value of a byte.
   *
   * java represents bytes as signed integers in two's complement.
   * To convert to the corresponding value of a byte representing an unsigned
   * integer we add 255 to negative values.
   *
   */
  public static byte uintToByte(int val){
    byte bval;
    if (val < 0) {
      throw new RuntimeException("value must be positive");
    }
    bval = val > 127 ? (byte)(-1*(256 -val)) : (byte)val;
    return bval;
  }

  /**
   * Convert an array of bytes to an array of integers.
   * If the size of the array isn't a multiple of 4 then we pad with zero bytes
   * to get a int which is 4 bytes.
   * The order is little endian [least_signifcant_byte, ..., most_significant byte].
   *
   * nums.length*4 must be >= data.length
   * @param data - the array of integers to convert
   * @param nums - Buffer into which to write the integers
   *
   */
  public static void bytesToInt(byte[] data, int[] nums) {

    if (nums.length*4 < data.length) {
      throw new RuntimeException("The buffer isn't large enough to hold data");
    }

    // How many full 4 bytes we have;
    int nfull = (int)Math.floor(data.length/4.0);
    int num_ints =(int)Math.ceil(data.length/4.0);

    // Mask which zeros out all bits added when byte is promoted
    // to an int.
    int mask = 0xFF;

    int int_index = 0;
    for (int offset = 0; offset + 4 <= data.length; offset+=4 ) {
      nums[int_index] = ((data[offset+3] & mask) << 24)
                   + ((data[offset+2] & mask) << 16)
                   + ((data[offset+1] & mask) << 8)
                   + (data[offset] & mask);
      int_index += 1;
    }

    // handle the last byte
    if (nfull < nums.length) {
      int_index = nums.length -1;
      nums[int_index] = 0;
      int shift = 0;
      int max_offset = data.length - 4 * nfull;
      int start = nfull * 4;
      for (int offset = 0; offset < max_offset; offset++ ) {
        shift = 8 * offset;
        nums[int_index] += ((data[start+offset] & mask) << shift);
      }
    }

    // Zero out any remaining ints
    for (int pos = num_ints; pos < nums.length; pos++){
      nums[pos] = 0;
    }
  }

  /**
   * Convert an array of bytes to an array of integers.
   * If the size of the array isn't a multiple of 4 then we pad with zero bytes
   * to get a int which is 4 bytes.
   * The order is little endian [least_signifcant_byte, ..., most_significant byte].
   */
  public static int[] bytesToInt(byte[] data) {
    // How many full 4 bytes we have;
    int nfull = (int)Math.floor(data.length/4.0);
    int[] nums = new int[(int)Math.ceil(data.length/4.0)];

    bytesToInt(data, nums);
    return nums;
  }


  /**
   * Convert an array of ints to an array of bytes.
   * The order is [least_signifcant_byte, ..., most_significant byte]
   *
   * @param data - The array of ints to read
   * @param buffer - The buffer to read into must have length >= data.length*4
   *
   * Data is read into an existing buffer to avoid reallocating.
   */
  public static void intsToBytes(int[] data, byte[] bytes) {

    if (data.length*4 > bytes.length) {
      throw new RuntimeException("Buffer is to small store the data");
    }

    int bindex = 0;
    for (int offset = 0; offset < data.length; offset +=1 ) {

      bytes[bindex] = (byte) (data[offset]);
      bytes[bindex+1] = (byte) (data[offset] >>> 8);
      bytes[bindex+2] = (byte) (data[offset] >>> 16);
      bytes[bindex+3] = (byte) (data[offset] >>> 24);
      bindex += 4;
    }
  }

  /**
   * Convert an array of ints to an array of bytes.
   * The order is [least_signifcant_byte, ..., most_significant byte]
   */
  public static byte[] intsToBytes(int[] data) {

    // Integer is 32 bits = 4 bytes.
    byte[] bytes = new byte[data.length*4];

    intsToBytes(data, bytes);
    return bytes;
  }
}
