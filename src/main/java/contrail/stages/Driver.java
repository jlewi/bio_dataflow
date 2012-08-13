package contrail.stages;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Driver {

	public static void main(String[] args){

		System.out.println("Test");

		FileWriter fstream;
		try {
			fstream = new FileWriter("/home/deepak/randomtest.txt",true);
			  BufferedWriter out = new BufferedWriter(fstream);
			  out.write("This is a test line");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
