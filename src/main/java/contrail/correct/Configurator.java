package contrail.correct;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.configuration.XMLConfiguration;



public class Configurator {
		
		// FLASH and QUAKE variables
		///////////////////////////////////////////////////
		static HashMap<String,String> manifestMap = new HashMap();
		// Setup Paths - To be Read from XML Config - deepak
		
		public static String Flash_Home = null;
		public static String Quake_Home = null;
		public static String Hadoop_Home = null;
		public static String Quake_Data = null;

		// HDFS Input Directory of Set 1 for Flash
		public static String Flash_Mate_1 = null;
		// Flattened Data from Set 1
		public static String Flash_Mate_1_Avro = null;
		
		//HDFS Input Directory of Set 2 (Set 1's Mates) for Flash
		public static String Flash_Mate_2 = null;
		//Flattened Data from Set 2
		public static String Flash_Mate_2_Avro = null;
		
		//Path on each node to its local Flash Input Data
		//Batched inputs for Flash Merging will be created at this path.
		
		public static String Flash_Node_In = null;
		//and output of flash at this path on each node..
		public static String Flash_Node_Out = null;
		public static String junkPath = null;
		
		// output of map-side join data..
		public static String Flash_Join_Out = null;
		public static String Quake_Join_Out = null;
		
		public static String Script_Home = null;
		public static String Quake_Node_In_Data = null;
		public static String Quake_Node_Out_Data = null;
		public static String Manifest = null;
		public static String Singles = null;
		
		public static String Quake_Mate_1 = null;
		public static String Quake_Mate_2 = null;
		
		public static String Quake_Mate_1_Avro = null;  ////--->Changed
		public static String Quake_Mate_2_Avro = null; //// --->Changed
		
		public static String Quake_Final_Out = null;
		public static String Quake_Singles_Out = null;
		
		public static String Flash_Final_Out = null;
		
		public static String Quake_Preprocess = null;
		public static String Quake_KmerCount = null;
		public static String Quake_Filtered_KmerCount = null;
		public static String BitHash_Input = null;
		public static String BitHash_Output = null;
		public static String BitHash_Local_Temp = null;
		public static String Quake_Mates_Out = null;
		public static String Non_Avro_Count_File = null; //--->changed
		public static String CorrectInDirectory = null;
		public static String Flash_Mate_1_Flat = null;
		
		public static String Flash_Mate_2_Flat = null;
		public static String Quake_Mate_1_Flat = null; 
		public static String Quake_Mate_2_Flat = null;
	////////////////////////////////////////////////////
		

		
	public Configurator(){
		
		//Present working directory
				//String pwd = System.getProperty("user.dir");
			
				//System.out.println("Hello World");
				
				
				XMLConfiguration config = new XMLConfiguration("config.xml");
						
				Flash_Home = config.getString("setup.flash_home");
				
				System.out.println(Flash_Home);
				Quake_Home = config.getString("setup.quake_home");
				Hadoop_Home = config.getString("setup.hadoop_home");
				Flash_Mate_1 = config.getString("hdfs_in_data.flash_mate_1");
				Flash_Mate_2 = config.getString("hdfs_in_data.flash_mate_2");
				Quake_Mate_1 = config.getString("hdfs_in_data.quake_mate_1");
				Quake_Mate_2 = config.getString("hdfs_in_data.quake_mate_2");
				Flash_Mate_1_Flat = config.getString("hdfs_out_data.flash_mate_1_flat");
				Flash_Mate_2_Flat = config.getString("hdfs_out_data.flash_mate_2_flat");
				Quake_Mate_1_Flat = config.getString("hdfs_out_data.quake_mate_1_flat");
				Quake_Mate_2_Flat = config.getString("hdfs_out_data.quake_mate_2_flat");
				Singles = config.getString("hdfs_in_data.singles");
				Quake_Singles_Out = config.getString("hdfs_out_data.quake_singles_out");
				Quake_Final_Out = config.getString("hdfs_out_data.quake_final_out");
				
				Flash_Node_In = config.getString("local_data.flash_input");
				
				Flash_Node_Out = config.getString("local_data.flash_output");
				Script_Home = config.getString("setup.script_home");
				 
				Flash_Join_Out = config.getString("hdfs_out_data.flash_join_out");
				Flash_Final_Out = config.getString("hdfs_out_data.flash_final_out");
				Quake_Join_Out = config.getString("hdfs_out_data.quake_join_out");
				Quake_Mates_Out = config.getString("hdfs_out_data.quake_mates_out");
				
				junkPath = config.getString("hdfs_out_data.junk");
				
				
				//@deepak - to do, remove hard path for manifest from config.xml
				Manifest = config.getString("local_data.manifest");
				
				// This method reads the Manifest File and Adds an Extra Column of Unique IDs.
				//This extra column helps in the MapSide Join by giving a column of unique IDs to join on.
				manifestRead();
				
				// the HDFS Input Directory for Running Quake's Correct.
				CorrectInDirectory = config.getString("hdfs_in_data.correctInputDirectory");
				Quake_Data = config.getString("local_data.quake_data");
				Quake_Preprocess = config.getString("hdfs_out_data.quake_preprocess");
				Quake_KmerCount = config.getString("hdfs_out_data.quake_kmercount");
				Quake_Filtered_KmerCount = config.getString("hdfs_out_data.quake_filtered_kmercount");
				BitHash_Output = config.getString("hdfs_out_data.bithash_output");
				BitHash_Local_Temp = config.getString("hdfs_out_data.bithash_local_temp");
				
				
				//BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream("/home/deepak/523/contrail-bio/src/main/resources/config.xml")));
				//System.out.println("ReadLine BF:" + bf.readLine());
		
	}
	
	public static void manifestRead() throws IOException
	{
		String manifestPath = Manifest;
		int count = 0;
		StringTokenizer st;
		String line;
		char uniqueString[] = new char[5];
		uniqueString[0]='a';
		uniqueString[1]='a';
		uniqueString[2]='a';
		uniqueString[3]='a';
		uniqueString[4]='a';
		int n=0;
		
		try
		{
			BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(manifestPath)));
			//System.out.print(bf.toString());
			
			
			do {
				line = bf.readLine();
				//System.out.println(line);
				
				if(line == null)break;
				count ++;
				if(count ==1)continue;
				st = new StringTokenizer(line);
				String fname1 = st.nextToken();
				String fname2 = st.nextToken();
				String overlap = st.nextToken();
				String do_flash = st.nextToken();
				String do_quake = st.nextToken();
				//System.out.print(fname1+"\n");
					int charToBeEdited = 4 - n/26;
					
					uniqueString[charToBeEdited] = (char)((int)(uniqueString[charToBeEdited]) +1); 
					
					String str ="";
					
					for(int i=0;i<5;i++)str+=uniqueString[i];
					String temp = overlap+"\t"+do_flash+"\t"+do_quake+"\t"+str+"\n";
					
					manifestMap.put(new String(fname1.trim()), temp);
					manifestMap.put(new String(fname2.trim()), temp);
					
					n++;
					
					
					
				
			}while(line != null);
		}
		catch(Exception e){}
		
		
		// Get Current Working Directory
		String curDir = System.getProperty("user.dir");
		
		 File fp = new File(Manifest+"_temp");
		 
		 
		 
		 if(fp.exists())fp.delete();

		 FileWriter fstream = new FileWriter(Manifest+"_temp",true);
		 BufferedWriter out = new BufferedWriter(fstream);
		 
		 for(String key: manifestMap.keySet())
		  {
			  
			  String uniqueName = manifestMap.get(key);
			  out.write(key+"\t"+uniqueName);
			  System.out.println(key+"\t"+uniqueName);
		  }
		 out.close();
		 fstream.close();
		
		 
		//  fp = new File(Manifest+"_temp");
		 // fp.renameTo(new File(Manifest));
	
	}
	
}