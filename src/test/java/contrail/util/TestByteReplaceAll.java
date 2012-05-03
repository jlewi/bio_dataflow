package contrail.util;

import java.lang.Math;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Unit test for ByteReplaceAll.
 *
 * @author jeremy@lewi.us <Jeremy Lewi>
 *
 */
public class TestByteReplaceAll {

	@Test
	public void testReplaceAll() throws Exception{

		Random generator = new Random();

		// The maximum value for a byte.
		// This must be a value of 127 to be compatible with ASCII.
		int MAX_BYTE = 127;
		// Create a random length string.
		// We do this by generating random integers between -128 and 127 and using
		// these values in a byte array. We then convert this array to a string.
		int max_length = 1000;
		int min_length = 10;
		int length = (int) Math.ceil(generator.nextFloat()*max_length);
		length = length > min_length ? length : min_length;

		byte[] byte_data = new byte[length];

		for (int index = 0 ; index < length; index ++){
			int val = (int) Math.ceil(generator.nextFloat()*MAX_BYTE);
			byte_data[index] = (byte) val;
		}

		String input_string = new String(byte_data, ByteReplaceAll.encoding);

		// Randomly determine how many characters to replace
		int nreplace = (int) Math.ceil(generator.nextFloat()*length/2.0);

		String match = "" ;
		for (int index = 0; index < nreplace ; index ++){
			int pos = (int) Math.floor(generator.nextFloat()*input_string.length());
			match += input_string.substring(pos,pos+1);
		}

		byte  new_value_byte = (byte)((int) Math.ceil(generator.nextFloat()*MAX_BYTE));

		String new_value = new String (new byte[]{new_value_byte});

		_run_test(byte_data, match, new_value);
	}

	@Test
	public void testEdgeCase (){
		// Use values at the end of the range
		byte[] byte_data = new byte[] {0x00, 0x00, 0x41, 0x7f, 0x41, 0x42, 0x43, 0x7f};
		String match = ByteUtil.bytesToString(new byte[] {0x00,0x7f}, ByteReplaceAll.encoding);
		String new_value = "z";

		_run_test(byte_data, match, new_value);
	}

	public void _run_test(byte[] byte_data, String match, String new_value)  {
		String input_string = ByteUtil.bytesToString(byte_data, ByteReplaceAll.encoding);

		// Construct the replacer object.
		ByteReplaceAll replacer = new ByteReplaceAll(match, new_value);

		// Replace the values in the byte array.
		byte[] out_byte_data = java.util.Arrays.copyOf(byte_data,byte_data.length);
		replacer.replaceAll(out_byte_data);

		//convert it to a string
		String output_string = ByteUtil.bytesToString(out_byte_data, ByteReplaceAll.encoding);

		// compute the correct output string
		String true_output_string = new String(input_string);
		for (int index = 0 ; index < match.length(); index++){
			true_output_string = true_output_string.replace(match.charAt(index), new_value.charAt(0));
		}

		//check the strings are equal
		assertEquals(output_string,true_output_string);
	}
}
