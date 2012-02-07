package contrail;

public class BinaryToChar {

	/**
	 * Do some experiments to test conversion from byte data to characters. 
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//Bew
		str_to_bytes();
		
	}
	
	public static void str_to_bytes() throws Exception {
		//Some tests for converting strings to bytes.
		
		String s  = "hello";
		byte[] b = s.getBytes("US-ASCII");
		System.out.println("done");
	}
	public static void split_tests(){
		//some tests I ran to try to understand the character code to use for
		// splitting lines
		int code = 0x80;
		char letter = (char)(code);
		System.out.println("Character is:" + letter);
		System.out.println("Integer value is:" + (int)letter);
		
		String concate = "hello" + letter + "world";
		String[] parts = concate.split(new String(new char[]{letter}));
		
		if (parts[0].compareTo("hello")==0){
			System.out.println("Correctly Split");			
		}
		else{
			System.out.println("InCorrectly Split");
		}
	}

}
