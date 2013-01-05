/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Avijit Gupta (mailforavijit@gmail.com)
package contrail.correct;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.MatePair;
import contrail.sequences.FastQRecord;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.FileHelper;
import org.apache.commons.io.FileUtils;
import java.util.*;

/**
 * Flash is a tool that is used to produce extended reads by combining reads from two mate pair files.
 * The input to this class is an AVRO file which contains MatePair records, which essentially contains 
 * two fastQ records.
 * Assumptions: 
 * 1 - Flash binary is available, and its path is specified in the parameters. This is used 
 * later to load the flash binary from the path into Distributed Cache
 * We have access to a system temp directoy on each node; i.e the java command File.createTempFile succeeds.
 * Execution:
 * As input we have mate pair records. A mate pair record is split into two fastQ records which are converted into two fastq records.
 * These records are written in blocks of blockSize into the local temporary files of cluster 
 * nodes. Flash is executed via exec and the results are collected. Care must be taken to clean 
 * blocks of files when they are of no use.
 */

public class InvokeFlash extends Stage {
  private static final Logger sLogger = Logger.getLogger(InvokeFlash.class);
  public static class RunFlashMapper extends AvroMapper<MatePair, FastQRecord>{	
    private String localOutFolderPath = null;
    private String flashHome = null;
    private String tempWritableFolder = null;
    private String blockFolder;
    private String notCombinedPath;
    private String jobName;
    private ArrayList<String> fastqRecordsMate1;
    private ArrayList<String> fastqRecordsMate2;
    private int count;
    private CorrectUtil correctUtil;
    private AvroCollector<FastQRecord> outputCollector;
    private int blockSize;
    @Override
    public void configure(JobConf job) {
      jobName = job.get("mapred.task.id");
      tempWritableFolder = FileHelper.createLocalTempDir().getAbsolutePath();
      System.out.println("The temp writable folder is: "+ tempWritableFolder);
      //Initialise empty array lists
      fastqRecordsMate1 = new ArrayList<String>();
      fastqRecordsMate2 = new ArrayList<String>();
      count = 0;
      outputCollector = null;
      correctUtil = new CorrectUtil();
      flashHome = correctUtil.getDcachePath("flash", job);
      System.out.println("flashHome is"+flashHome);
      InvokeFlash stage = new InvokeFlash();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      blockSize = (Integer)(definitions.get("block_size").parseJobConf(job));
      System.out.println("The blockSize is: "+ blockSize);
     // System.out.println("notcombinedpath is: "+ notCombinedPath);
      notCombinedPath = (String)(definitions.get("flash_notcombined").parseJobConf(job));
      System.out.println("notcombinedpath is: "+ notCombinedPath);
      
      sLogger.info("Flash Home: " + flashHome); 
  }
  
  public void map(MatePair mateRecord, 
                  AvroCollector<FastQRecord> collector, Reporter reporter) throws IOException {
    if(outputCollector == null){
      outputCollector = collector;
    }
    count++;
    
    correctUtil.addMateToArrayLists(mateRecord, fastqRecordsMate1, fastqRecordsMate2);
    // Time to process one block
    if(count ==blockSize){
      System.out.println("Time to Process one Block");
      runFlashOnInMemoryReads(collector);
      count = 0;
    }
  }
  
  /**
   * This method runs flash locally and collects the results.
   * @param output: The reference of the collector
   * @throws IOException
   */
  private void runFlashOnInMemoryReads(AvroCollector<FastQRecord> collector)throws IOException {
    
    System.out.println("In FlashOnInMemoryReads function");
    String filePathFq1;
    String filePathFq2;
    //gets the current timestamp in nanoseconds
    long time = System.nanoTime();
    
    // blockSize number of reads are written to a temporary folder - blockFolder
    // blockFolder is a combination of mapred taskid and timestamp to ensure uniqueness 
    // The input files are names <timestamp_1>.fq and <timestamp>_2.fq. Flash is executed
    // on these and the output file produced is out.extendedFrags.fastq in the same directory.
    // During cleanup, we can delete the blockFolder directly
     
    blockFolder = jobName+time;
    
    localOutFolderPath = new File(tempWritableFolder,blockFolder).getAbsolutePath();
    File tempFile = new File(localOutFolderPath);
    if(!tempFile.exists()){
      tempFile.mkdir();
    }
    
    filePathFq1 = new File(localOutFolderPath,time + "_1.fq").getAbsolutePath();
    filePathFq2 = new File(localOutFolderPath,time + "_2.fq").getAbsolutePath();
    
    correctUtil.writeLocalFile(fastqRecordsMate1,filePathFq1); 
    correctUtil.writeLocalFile(fastqRecordsMate2,filePathFq2); 
    
    
    fastqRecordsMate1.clear();
    fastqRecordsMate2.clear();
    
    
    
    String command = flashHome+" "+filePathFq1+" "+filePathFq2 + " -d "+ localOutFolderPath;
    
    
    System.out.println("Command Getting Executed For Flash: ");
    System.out.println(command);
    correctUtil.executeCommand(command);
    
    
    
    String combinedFilePath = localOutFolderPath + "/out.extendedFrags.fastq";	
      
    String notCombined_1_String = localOutFolderPath+"/out.notCombined_1.fastq";
    System.out.println("notCombined_1 is: "+ notCombined_1_String);
   
    String notCombined_2_String= localOutFolderPath+"/out.notCombined_2.fastq";
    System.out.println("notCombined_2 is: "+ notCombined_2_String);
    
    
    
    File test1 = new File(notCombined_2_String);
    File test2 = new File(combinedFilePath);
    File test3 = new File(notCombined_2_String);
    
    File test4 = new File(filePathFq2);
    File test5 = new File(filePathFq1);
    
    System.out.println(test4.length() +"and the other guy's length is:"+ test5.length());
    
    if (test4.exists())
      System.out.println("First fastq block file is getting created");
    
    System.out.println("File Existence Check!");
    
    if (test1.exists() || test2.exists() || test3.exists())
      System.out.println("|" + test1.exists() + "exists or not" + test2.exists() + "exists or not-" + test3.exists());
       
    Configuration conf = new Configuration();
    FileSystem hdfs = FileSystem.get(conf);

    tempFile = new File(combinedFilePath);
    
    File combinedFile = new File(combinedFilePath);
    File notCombined_1 = new File(notCombined_1_String);
    // collecting results of extended file
    //correctUtil.emitFastqFileToHDFS(combinedFile, collector);
    correctUtil.emitFastqFileToHDFS(notCombined_1, collector);
    // copy the two not combined files to HDFS. We do this after we have collected the combined reads,
    // so that if the Mapper fails before this point, we don't copy the not combined files unnecessarily.
    
    hdfs.copyFromLocalFile(new Path(notCombined_1_String), new Path(notCombinedPath,blockFolder+"_1.fq"));
    hdfs.copyFromLocalFile(new Path(notCombined_2_String), new Path(notCombinedPath,blockFolder+"_2.fq"));
    
    // Cleaning up the block Folder. The results of the extended file have been collected.
    tempFile = new File(localOutFolderPath);
    if(tempFile.exists()){
      FileUtils.deleteDirectory(tempFile);
    }
  }
  
  /**
   * Writes out the remaining chunk of data which is a non multiple of blockSize
   */
  public void close() throws IOException{
    if(count > 0){
      
      System.out.println("From Close Function()");
      runFlashOnInMemoryReads(outputCollector);
    }
    //delete the top level directory, and everything beneath
    File tempFile = new File(tempWritableFolder);
    /*
    if(tempFile.exists()){
    	FileUtils.deleteDirectory(tempFile);    
    }*/
  }
}
	
  /* creates the custom definitions that we need for this phase*/
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    
    ParameterDefinition notCombinedFlash = new ParameterDefinition(
    "flash_notcombined", "HDFS path of reads not combined by flash", String.class, new String(""));
    
    ParameterDefinition flashBinary = new ParameterDefinition(
    "flash_binary", "The path of flash binary ", String.class, new String(""));
    
    ParameterDefinition blockSize = new ParameterDefinition(
        "block_size", "block_size number of records are" + 
        " written to local files at a time.", Integer.class, new Integer(10000));
    
    for (ParameterDefinition def: new ParameterDefinition[] {flashBinary, blockSize, notCombinedFlash}) {
      defs.put(def.getName(), def);
    }
    
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    
    return Collections.unmodifiableMap(defs);
  }

  public RunningJob runJob() throws Exception {
    JobConf conf = new JobConf(InvokeFlash.class);
    conf.setJobName("Flash invocation");
    String[] required_args = {"inputpath", "outputpath","flash_binary","flash_notcombined"};
    checkHasParametersOrDie(required_args);   
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");    
    String flashPath = (String) stage_options.get("flash_binary");   
    String notCombinedPath = (String) stage_options.get("flash_notcombined");
    
    /*
    if (flashPath.length() == 0 || notCombinedPath.length() == 0) {
      throw new Exception("Flash binary location not given, or path for uncombined reads not given");
    }
    */
    
    DistributedCache.addCacheFile(new Path(flashPath).toUri(),conf);
    //Sets the parameters in JobConf
    initializeJobConfiguration(conf);
    AvroJob.setMapperClass(conf, RunFlashMapper.class);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    
    //Input  
    MatePair read = new MatePair();
    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setOutputSchema(conf, new FastQRecord().getSchema());
    
    //Map Only Job
    conf.setNumReduceTasks(0);
    
    FileSystem hdfs = FileSystem.get(conf);
    
    
    
    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (hdfs.exists(out_path)) {
      hdfs.delete(out_path, true);  
    }	    
    
    if (hdfs.exists(new Path(notCombinedPath))) {
      hdfs.delete(new Path(notCombinedPath), true);
    }
    
    hdfs.mkdirs(new Path(notCombinedPath));
    
    long starttime = System.currentTimeMillis();            
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return result;	
  }
	
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvokeFlash(), args);
    System.exit(res);
  }
}
