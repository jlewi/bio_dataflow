package contrail;

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
	 * @return - The number of bytes in the resulting array after replacing
	 *           multi-byte characters with new_value.
	 */
	public static int replaceMultiByteChars(byte[] data, byte new_value){
		if ( (new_value & 0x80) == 0x80) {
			throw new RuntimeException("The new value doesn't correspond to a single byte character in UTF-8");
		}
		// Keep track of the indexes corresponding to 
		// where we should read/write the next bytes.
		int read=0;
		int write=0;
		
		while (read < data.length){
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
					if (read == data.length){
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
}
