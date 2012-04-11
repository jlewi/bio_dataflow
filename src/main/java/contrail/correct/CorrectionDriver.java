package contrail.correct;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class CorrectionDriver {

	public static void main(String[] args) throws Exception {
		

	    	
	    	Configurator corConf = new Configurator();
	    	
	    	
	    	boolean convertFlashInputToAvro = true;
	    	boolean sortFiles = false;
	    	boolean convertQuakeInputToAvro = false;
	    	boolean simulateJoin = false;
	    	boolean kmercount =true;
	    	boolean avroToNonAvroPartFile = true;
	    	boolean calculateCutOff = true;
	    	boolean pruneCountFile = true;
	    	boolean calculateBitHash = true;
	    	boolean createGlobalBitHash = true;
	    	boolean quakecompatiblebh = true;
	    	boolean correctForSingles = true;
	    	boolean runFlash = true;
	    	boolean run_paired_quake = true;
	    	boolean runJoinFlash ;
	    	boolean deleteFlashLog ;
	    	runJoinFlash = deleteFlashLog = true;
	    	boolean runJoinQuake ;
	    	boolean deleteQuakeLog ;
	    	runJoinQuake = deleteQuakeLog = true;
	    	
	    	int K =13;
	    	
	    	
	    	// @deepak - to do - set the global K value from error correction. 
	    	//ContrailConfig.K = K;
	    	
	    	if(convertFlashInputToAvro) {
	    		System.out.println("Converting Files into Avro");
	    		FastQtoAvro fqta = new FastQtoAvro();
	    		
	    		//fqta.run(corConf.Flash_Mate_1_Avro, corConf.Flash_Mate_1_Avro);
	    		//fqta.run(corConf.Flash_Mate_2, corConf.Flash_Mate_2_Avro);
	    		
	    		//fqta.run(corConf.Quake_Mate_1, corConf.Quake_Mate_1_Avro);
	    		//fqta.run(corConf.Quake_Mate_2, corConf.Quake_Mate_2_Avro);

	    		fqta.run("singles_input", corConf.Singles);
	    	}
	    	
	   
	}
}
	    	
	    	
	    	
	    	/*if(sortFiles)
	    	{
	    		SortAvroFile obj = new SortAvroFile();
	    		obj.run(ContrailConfig.Flash_Mate_1_Avro, "ContrailPlus/Flash_Avro_1_Sorted");
	    		obj.run(ContrailConfig.Flash_Mate_2_Avro, "ContrailPlus/Flash_Avro_2_Sorted");
	    	}
	    	*/
	    	/*if(simulateJoin)
	    	{
	    		JoinSimulator js = new JoinSimulator();
	    		js.run(ContrailConfig.Singles, ContrailConfig.Flash_Join_Out);
	    	}
	    	*/
	    	
	    	/*
	    	if(runJoinFlash)
	    	{
	    		 System.out.println("Joining For Flash");
	    		 joinGenerator.generateJoin(ContrailConfig.Flash_Join_Out, "flash");
	    	}
	    	 if(deleteFlashLog)
	         {
	    		 System.out.println("Deleting Flash Log");
	             String logPath = ContrailConfig.Flash_Join_Out+"/_logs";
	             String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -rmr " + logPath;    
	             try {
	                
	                 Process p = Runtime.getRuntime().exec(command);
	                 BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                 String s;
	                
	                  while ((s = stdInput.readLine()) != null) {
	                      System.out.print("");
	                  }
	                
	             } catch (IOException e) {
	                 // TODO Auto-generated catch block
	                 e.printStackTrace();
	             }
	             
	         }
	        
	    	 
	    	 
	    	if(runFlash)
	    	{
	   		 System.out.println("Running Flash");

	    		String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -rmr "+ContrailConfig.Flash_Final_Out;
	      		 System.out.println(command);

	        	try {
	                 
	                 Process p = Runtime.getRuntime().exec(command);
	                 BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                 String s;
	                  while ((s = stdInput.readLine()) != null) {
	                      System.out.print("");
	                  }
	                
	             } catch (IOException e) {
	                 // TODO Auto-generated catch block
	                 e.printStackTrace();
	             }
	             
	             String command2 = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -mkdir "+ContrailConfig.Flash_Final_Out;
	       		 System.out.println(command2);


	             try {
	                 
	                 Process p = Runtime.getRuntime().exec(command2);
	                 BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                 
	                 String s;

	                  while ((s = stdInput.readLine()) != null) {
	                      System.out.print("");
	                  }
	                
	             } catch (IOException e) {
	                 // TODO Auto-generated catch block
	                 e.printStackTrace();
	             }
	             System.out.println("Flash Paired Files createtion");
	             // This is the key method here ExportToPairedFiles() - It triggers a MapReduce Job to first write flash input data on each node, 
	         	 //and then runs flash across the cluster.
	             createPairedReadsForFlash.run(ContrailConfig.Flash_Join_Out, ContrailConfig.junkPath);
	    	}
	    	
	    	
	    	if(kmercount)
	    	{
	    		System.out.println("Counting Kmers");
	    		
	    		kmerCounter k = new kmerCounter();
	    		k.run(ContrailConfig.Singles, ContrailConfig.Quake_KmerCount, K);
	    	}
	    	   ///Count File Creation from Avro
	    	
	    	if(avroToNonAvroPartFile)
	    	{
	    		System.out.println("Converting Part File to Non Avro");
	    		
	    		convertPartFileToNonAvro conv = new convertPartFileToNonAvro();
	    		conv.run(ContrailConfig.Quake_KmerCount+"/part-00000.avro", ContrailConfig.Non_Avro_Count_File);
	    	}  
	    	
	    	if(calculateCutOff)
	    	{
	    		System.out.println("Calculating Cutoff");

	    		ContrailConfig.cutoff = cutOffCalculation.calculateCutoff();
	    		System.out.print("\nCutoff Here= "+ContrailConfig.cutoff);
	    	}
	    	
	    	//ContrailConfig.cutoff = 3; /// Remove this after testing
	    	if(pruneCountFile)
	    	{
	    		System.out.println("Pruning Counts File");

	            FilterGlobalCountFile.FilterKmers(ContrailConfig.Quake_KmerCount,ContrailConfig.Quake_Filtered_KmerCount,ContrailConfig.cutoff);

	    	}
	    	//////Working till here!
	    	
	    	if(calculateBitHash)
	    	{
	    		System.out.println("Calculating Bithash Local");
	    		localBitHash.bitHashlocal(ContrailConfig.Quake_Filtered_KmerCount, ContrailConfig.junkPath, K, ContrailConfig.cutoff);
	    	}
	    	//rec.run("inputfq","outputfq");
	    	if(createGlobalBitHash)
	    	{
	    		System.out.println("Generating Numeric Bithash Global ");
	            CreateGlobalBitHash.BitMapGenerator(ContrailConfig.BitHash_Local_Temp, ContrailConfig.BitHash_Output);
	    	}
	    	if(quakecompatiblebh)
	    	{
	    		System.out.println("Generating Quake compatible Bithash");
	    		generateQuakeCompatibleBithash.run();
	            
	    	}
	    	
	    	if(correctForSingles)
	    	{
	    		
	    		String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -rmr "+ContrailConfig.Quake_Singles_Out;
	         	
	         	try {
	                  
	                  Process p = Runtime.getRuntime().exec(command);
	                  BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                  String s;
	            
	                   while ((s = stdInput.readLine()) != null) {
	                       System.out.print("");
	                   }
	                 
	              } catch (IOException e) {
	                  // TODO Auto-generated catch block
	                  e.printStackTrace();
	              }
	         	
	    		command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -mkdir "+ContrailConfig.Quake_Singles_Out;
	         	
	         	try {
	                  
	                  Process p = Runtime.getRuntime().exec(command);
	                  BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                  String s;
	            
	                   while ((s = stdInput.readLine()) != null) {
	                       System.out.print("");
	                   }
	                 
	              } catch (IOException e) {
	                  // TODO Auto-generated catch block
	                  e.printStackTrace();
	              }
	         	
	    		
	    		String paths = ContrailConfig.Singles+","+ContrailConfig.Flash_Final_Out;
	    		System.out.println("Running Correct for singles");

	    		//String paths = ContrailConfig.Singles;
	    		CorrectSinglesInvocationStub.run(paths,ContrailConfig.junkPath);
	    	}
	    	
	    	if(runJoinQuake)
	    	{
	    		System.out.println("Generating Quake Join");
	    		 joinGenerator.generateJoin(ContrailConfig.Quake_Join_Out, "quake");
	    	}
	    	 if(deleteQuakeLog)
	         {
	     		System.out.println("Deleting Quake Join Log");

	             String logPath = ContrailConfig.Quake_Join_Out+"/_logs";
	             String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -rmr " + logPath;    
	             try {
	                
	                 Process p = Runtime.getRuntime().exec(command);
	                 BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                 String s;
	                
	                  while ((s = stdInput.readLine()) != null) {
	                      System.out.print("");
	                  }
	                
	             } catch (IOException e) {
	                 // TODO Auto-generated catch block
	                 e.printStackTrace();
	             }
	             
	         }
	    	
	    	if(run_paired_quake)
	    	{
	    		System.out.println("Running Quake on Paired Reads");

	    		String command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -rmr "+ContrailConfig.Quake_Final_Out;
	         	
	         	try {
	                  
	                  Process p = Runtime.getRuntime().exec(command);
	                  BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                  String s;
	            
	                   while ((s = stdInput.readLine()) != null) {
	                       System.out.print("");
	                   }
	                 
	              } catch (IOException e) {
	                  // TODO Auto-generated catch block
	                  e.printStackTrace();
	              }
	         	
	         	command = ContrailConfig.Hadoop_Home+"/bin/hadoop dfs -mkdir "+ContrailConfig.Quake_Final_Out;
	         	
	         	try {
	                  
	                  Process p = Runtime.getRuntime().exec(command);
	                  BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
	                  String s;
	            
	                   while ((s = stdInput.readLine()) != null) {
	                       System.out.print("");
	                   }
	                 
	              } catch (IOException e) {
	                  // TODO Auto-generated catch block
	                  e.printStackTrace();
	              }
	         	
	         	System.out.println("Quaked Paired Files creation");
	         	createPairedReadsForQuake.createPairedReads(ContrailConfig.Quake_Join_Out, ContrailConfig.junkPath);
	    	}
	    	
	    	
	       } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/