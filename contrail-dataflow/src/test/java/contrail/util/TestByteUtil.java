package contrail.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

public class TestByteUtil {

  final int BITSINBYTE = 8;
  @Test
  public void testByteToUint(){
    // Test the conversion of bytes to uints and back again.
    // Test conversion to byte value is correct.
    for (int num = 0 ; num < 255; num++) {
      byte bval = ByteUtil.uintToByte(num);

      // Compute the unsigned integer value of this byte
      byte bmask = 0x1;

      int power = 1;

      int bval_uint = 0;
      for (int bit_pos = 0; bit_pos < BITSINBYTE; bit_pos++ ){
        // pick of just the first bit and multiply it by power
        bval_uint += ((int)(bmask & bval)) * power;

        // Shift bval to the right and double power
        power *= 2;
        int shift = 1;
        bval = (byte)(bval >>> shift);

      }
      assertEquals(bval_uint, num);

      bval = ByteUtil.uintToByte(num);
      assertEquals(num, ByteUtil.byteToUint(bval));
    }

  }
	@Test
	public void testhasMultiByteChars(){
		// Test whether a sequence of bytes contains any multi byte characters.

		// Create a multi byte sequence.
		String multi_seq = "abcd\u5639\u563b";

		byte[] multi_byte_seq;
		byte[] single_byte_seq;
		try{
			multi_byte_seq = multi_seq.getBytes("UTF-8");
			single_byte_seq = "abWu".getBytes("UTF-8");

			assertTrue(ByteUtil.hasMultiByteChars(multi_byte_seq));
			assertFalse(ByteUtil.hasMultiByteChars(single_byte_seq));
		}
		catch (java.io.UnsupportedEncodingException e){
		}
	}

	@Test
	public void testreplaceMultiByteChars() throws UnsupportedEncodingException {
		// TODO(jlewi): We should really use randomly
		// generated strings.

		// Create pairs of strings to test.
		// First value is the original string,
		// second value is the correct string
		class Tuple {
			String input;
			String output;

			public Tuple(String in, String out){
				input = in;
				output = out;
			}
		}
		java.util.ArrayList<Tuple> cases = new java.util.ArrayList<Tuple>();

		cases.add(new Tuple("abcd\u5639\u563b","abcd__"));
		cases.add(new Tuple("abcd\u5639w\u563b","abcd_w_"));
		cases.add(new Tuple("\u5639w\u563b","_w_"));
		cases.add(new Tuple("\u0130w\u563b","_w_"));

		byte new_value = "_".getBytes("UTF-8")[0];

		for (int index = 0; index < cases.size(); index++) {
			Tuple data = cases.get(index);

			byte[] in_bytes;
			try {
				in_bytes = data.input.getBytes("UTF-8");
			}
			catch (java.io.UnsupportedEncodingException e){
				throw new RuntimeException("Problem getting bytes");
			}
			int length =ByteUtil.replaceMultiByteChars(in_bytes, new_value);

			assertEquals(length,data.output.length());


			String output = new String(in_bytes,0,length,"UTF-8");
			assertEquals(output,data.output);
		}
	}

	@Test
  public void testbytesToInts() {
	  // Test that the converstion of an array of bytes to ints is correct.

	  // test it for the following lengts of arrays of bytes. This range ensures we cover all possible
	  // cases of padding.
	  for (int blength = 1; blength <= 12; blength++) {
	    // Generate a random array of bytes.
	    byte[] bdata = new byte[blength];
	    for (int index = 0; index < blength; index++){
	      bdata[index] = ByteUtil.uintToByte((int)Math.floor(Math.random()*256));
	    }

	    int[] nums = ByteUtil.bytesToInt(bdata);

	    assertEquals(nums.length, (int)Math.ceil(bdata.length/4.0));
	    for (int bindex = 0; bindex < blength ; bindex++) {
	      // Determine the offset
	      int nfull = (int) Math.floor(bindex/4.0);
	      int offset = bindex % 4;

	      int nshift = offset * 8;
	      byte int_byte = (byte) ((nums [nfull] >>> nshift) & 0xFF);
	      assertEquals(int_byte, bdata[bindex]);
	    }

	    // test that int to bytes returns the inverse value
	    byte[] byte_check = ByteUtil.intsToBytes(nums);

	    for (int pos = 0; pos < bdata.length; pos++){
	      assertEquals(byte_check[pos], bdata[pos]);
	    }
	    // Check that an extra bytes are zeros.
	    for (int pos = bdata.length; pos < byte_check.length; pos++){
	      assertEquals(byte_check[pos], 0);
	    }
	  }
	}
  @Test
  public void testIntsToBytes() {
    // Test that the converstion of an array of ints to bytes is correct.

    //
    for (int int_length = 1; int_length <= 5; int_length++) {
      // Generate a random array of bytes.
      int[] idata = new int[int_length];
      for (int index = 0; index < int_length; index++){
        long range = java.lang.Integer.MAX_VALUE - java.lang.Integer.MIN_VALUE;
        idata[index] = (int)Math.floor((Math.random() * range) + java.lang.Integer.MIN_VALUE);
      }

      byte[] bytes = ByteUtil.intsToBytes(idata);

      assertEquals(bytes.length, 4 * idata.length);
      for (int bindex = 0; bindex+4 <= bytes.length ; bindex+=4) {
        int num = 0;
        num =  ((bytes[bindex] & 0xFF))
            +  ((bytes[bindex+1] & 0xFF) << 8)
            +  ((bytes[bindex+2] & 0xFF) << 16)
            +  ((bytes[bindex+3] & 0xFF) << 24);
        assertEquals(num, idata[bindex/4]);
      }
    }
  }
}
