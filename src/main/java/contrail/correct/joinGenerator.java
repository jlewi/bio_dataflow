package contrail.correct;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class joinGenerator {
/* Invokes the pig script*/

	
	public static void generateJoin(String joinedFilePath, String cmd) throws Exception 
	{
		//delete mergedpath if it exists for the pig script 

		//@to do - remove the hard shell api call and replace with hdfs api call to delete.
		/*
		JobConf job = new JobConf();
		  //delete the output directory if it exists already
	    Path out_path = new Path(joinedFilePath);
	    if (FileSystem.get(job).exists(out_path)) {
	      FileSystem.get(job).delete(out_path, true);  
	    }
		*/
		
		
		Configurator corConf = new Configurator();
		
		String delCommand = corConf.Hadoop_Home + "/bin/hadoop dfs -rmr "+joinedFilePath; 
	
		try {
			//System.out.print();
			Process p = Runtime.getRuntime().exec(delCommand);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s;
			System.out.println("Output of Pig Run");
			 while ((s = stdInput.readLine()) != null) {
				 System.out.println(delCommand);
			 }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		String pig_file_path = null;
		String command = null;
		
		if ( cmd.equals("flash"))
		{
			pig_file_path = corConf.Script_Home+"/flashjoin.pig";
			command = "pig " + pig_file_path;
		}
		
		else
		{
			pig_file_path = corConf.Script_Home+"/quakejoin.pig";
			command = "pig "+ pig_file_path;
			
		}
		
		System.out.println(command);
		try {
			
			Process p = Runtime.getRuntime().exec(command);
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String s;
			
			 while ((s = stdInput.readLine()) != null) {
				 System.out.println(command);
			 }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}


