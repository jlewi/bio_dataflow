package contrail;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

public class TestByteUtil {

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
	public void testreplaceMultiByteChars() throws UnsupportedEncodingException{
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
}
