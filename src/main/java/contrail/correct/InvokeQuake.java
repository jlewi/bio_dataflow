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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
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
import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.FileHelper;


/**
 * Quake is a tool that is used to correct fastq reads. This class deals with correction of both 
 * paired reads (MatePair) and unpaired reads (FastQRecord).
 * The input to this class is an AVRO file might be either of MatePair type (which essentially contains 
 * two fastQ records) or FastQRecord type. 
 *   
 * Assumptions: 
 * 1 - Quake binary is available, and its path specified in the parameters. This is used 
 * later to load the flash binary from the location into Distributed Cache
 * We have access to a system temp directoy on each node; i.e the java command File.createTempFile succeeds.
 * The bitvector of kmers above the cutoff has been constructed and is available at a location 
 * specified in the input
 * Execution:
 * For the AVRO file in MatePair schema:
 * A mate pair record is split into two fastQ records which are converted into two fastq records.
 * These records are written in blocks of blockSize onto the local temporary files of cluster 
 * nodes. Quake is executed via exec and the results are collected. Care must be taken to clean 
 * blocks of files when they are of no use.
 * For the AVRO file in FastqRecord schema:
 * fastq records are written in blocks of blockSize onto the local temporary files of cluster 
 * nodes. Quake is executed via exec and the results are collected. Care must be taken to clean 
 * blocks of temporary files when they are of no use.
 */ 
public class InvokeQuake extends Stage{
  private static final Logger sLogger = Logger.getLogger(InvokeQuake.class);
  
  public static class RunQuakeMapper extends AvroMapper<Object, FastQRecord>{
    private String localOutFolderPath = null;
    private String quakeHome = null;
    private String tempWritableFolder = null;
    private String bitVectorPath = null;
    private String jobName;
    // fastqRecordsMateLeft & fastqRecordsMateRight are used in case the input
    // AVRO file follows the MatePair schema
    private ArrayList<String> fastqRecordsMateLeft;
    private ArrayList<String> fastqRecordsMateRight;
    // fastqRecordList is used in case the input AVRO file follows FastQRecord schema
    private ArrayList<String> fastqRecordList;
    private int K;
    private String blockFolder;
    private int count;
    private int blockSize;
    private CorrectUtil correctUtil;
    private AvroCollector<FastQRecord> outputCollector;
    private boolean matePairMode;
    
    @Override
    public void configure(JobConf job){
    InvokeQuake stage = new InvokeQuake(); 
      jobName = job.get("mapred.task.id");
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      tempWritableFolder = FileHelper.createLocalTempDir().getAbsolutePath();
      K = (Integer)(definitions.get("K").parseJobConf(job));  
      blockSize = (Integer)(definitions.get("block_size").parseJobConf(job));
      sLogger.info("blockSize "+ blockSize);
      correctUtil = new CorrectUtil();
      // Initialize empty mate pair ArrayLists 
      fastqRecordsMateLeft= new ArrayList<String>();
      fastqRecordsMateRight= new ArrayList<String>();
      // Initialize empty array list 
      fastqRecordList= new ArrayList<String>();
      count = 0;
      outputCollector = null;
      // We initially initialize matePairMode as false
      // We will set it if we encounter a record of type MatePair
      // The false value indicates the input record is of type FastQRecord
      // We need this flag in order to distinguish the mate pairs from fastqrecords
      // during close
      matePairMode = false;
      // gets the dcache path of file named correct
      quakeHome = correctUtil.getDcachePath("correct", job);
      // gets the dcache path of the file named bitvector
      bitVectorPath = correctUtil.getDcachePath("bitvector", job);
      if(quakeHome.length() == 0){
        sLogger.error("Error in reading binary from Dcache");
      }
      if(bitVectorPath.length() == 0){
        sLogger.error("Error in reading bitvector from Dcache");
      }
      sLogger.info("Quake Home: " + quakeHome);  
      sLogger.info("BitVector Location: " + bitVectorPath);
      
    }
    
    public void map(Object record, 
                    AvroCollector<FastQRecord> collector, Reporter reporter) throws IOException {
      count++;
      if(record instanceof FastQRecord){
        FastQRecord fastqRecord = (FastQRecord)record;
        if(outputCollector == null){
          outputCollector = collector;
        }
        fastqRecordList.add(correctUtil.fastqRecordToString(fastqRecord));
        // Time to process one block
        if(count ==blockSize){
          runQuakeSingleOnInMemoryReads(collector);
          count = 0;
        }
      }
      
      if(record instanceof MatePair){
        matePairMode = true;
        MatePair mateRecord = (MatePair)record;
        if(outputCollector == null){
          outputCollector = collector;
        }
        fastqRecordsMateLeft.add(correctUtil.fastqRecordToString(mateRecord.getLeft()));
        fastqRecordsMateRight.add(correctUtil.fastqRecordToString(mateRecord.getRight()));
        // Time to process one block
        if(count ==blockSize){
          runQuakePairedOnInMemoryReads(collector);
          count = 0;
        }
      }
      
    }
    
    /**
     * Creates a folder within the job's temporary directory named jobName + suffix where
     * the suffix is the input parameter 
     * @param folderNameSuffixString
     * @return
     */
    private String createBlockFolder(String folderNameSuffixString){
      // blockSize number of reads are written to a temporary folder - blockFolder
      // blockFolder is a combination of mapred taskid and timestamp to ensure uniqueness 
      // The input files are names <timestamp_1>.fq and <timestamp>_2.fq. Flash is executed
      // on these and the output file produced is out.extendedFrags.fastq in the same directory.
      // During cleanup, we can delete the blockFolder directly
      
      blockFolder = jobName+folderNameSuffixString;
      sLogger.info("block folder: " + blockFolder);
      // Create temp file names
      String folderPath = new File(tempWritableFolder,blockFolder).getAbsolutePath();
      sLogger.info("local out folder: " + folderPath);
      File tempFile = new File(folderPath);
      if(!tempFile.exists()){
        tempFile.mkdir();
      }
      return folderPath;
    }
    
    /**
     * This method runs quake locally and collects the results.
     * @param output: The reference of the collector
     * @throws IOException
     */
    private void runQuakeSingleOnInMemoryReads(AvroCollector<FastQRecord> output)throws IOException {
      String filePathFq;
      // Converts the timestamp to a string. 
      String localTime = new Long(System.nanoTime()).toString();
      localOutFolderPath = createBlockFolder(localTime);
      filePathFq = new File(localOutFolderPath,localTime + ".fq").getAbsolutePath();
      correctUtil.writeLocalFile(fastqRecordList,filePathFq);  
      
      String fastqListLocation = new File(localOutFolderPath, localTime+".txt").getAbsolutePath();
      correctUtil.writeStringToFile(filePathFq, fastqListLocation);
      // Correction command
      String command = quakeHome + " -f " + fastqListLocation + " -k " + K + " -b " + bitVectorPath;
      correctUtil.executeCommand(command);  
      fastqRecordList.clear();
      String correctedFilePath = FilenameUtils.removeExtension(filePathFq) + ".cor.fq";
      sLogger.info("corrected path: " + correctedFilePath);
      correctUtil.emitFastqFileToHDFS(new File(correctedFilePath), output);
      // Clear temporary files
      File tempFile = new File(localOutFolderPath);
      if(tempFile.exists()){
        FileUtils.deleteDirectory(tempFile);
      }
    }
    
    /**
     * This method runs quake locally and collects the results.
     * @param output: The reference of the collector
     * @throws IOException
     */
    private void runQuakePairedOnInMemoryReads(AvroCollector<FastQRecord> collector)throws IOException {
      String filePathFq1;
      String filePathFq2;
      
      // Converts the timestamp to a string. 
      String localTime = new Long(System.nanoTime()).toString();
      localOutFolderPath = createBlockFolder(localTime);
      filePathFq1 = new File(localOutFolderPath, localTime + "_1.fq").getAbsolutePath();
      filePathFq2 = new File(localOutFolderPath, localTime + "_2.fq").getAbsolutePath();
      correctUtil.writeLocalFile(fastqRecordsMateLeft,filePathFq1);
      correctUtil.writeLocalFile(fastqRecordsMateRight,filePathFq2);
      String fastqListLocation = new File(localOutFolderPath,localTime+".txt").getAbsolutePath();
      String data = filePathFq1 + " " + filePathFq2 + "\n";
      correctUtil.writeStringToFile(data, fastqListLocation);
      String command = quakeHome + " -f " + fastqListLocation + " -k " + K + " -b " + bitVectorPath;
      correctUtil.executeCommand(command);
      fastqRecordsMateLeft.clear();
      fastqRecordsMateRight.clear();
      String correctedFilePathLeft = FilenameUtils.removeExtension(filePathFq1) + ".cor.fq";
      correctUtil.emitFastqFileToHDFS(new File(correctedFilePathLeft), collector);
      String correctedFilePathRight = FilenameUtils.removeExtension(filePathFq1)  + ".cor.fq";
      correctUtil.emitFastqFileToHDFS(new File(correctedFilePathRight), collector);
      // Clear temporary files
      File tempFile = new File(localOutFolderPath);
      if(tempFile.exists()){
        FileUtils.deleteDirectory(tempFile);
      }
    }
    
    /**
     * Writes out the remaining chunk of data which is a non multiple of blockSize
     */
    public void close() throws IOException{
      if(count > 0){
        if(matePairMode){
            runQuakePairedOnInMemoryReads(outputCollector);
        }
        else{
              runQuakeSingleOnInMemoryReads(outputCollector); 
        }
      }
      // delete the top level directory, and everything beneath
      File tempFile = new File(tempWritableFolder);
      if(tempFile.exists()){
          FileUtils.deleteDirectory(tempFile);
      }
    }
  }

  /**
   *  creates the custom definitions that we need for this phase
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition quakeBinary = new ParameterDefinition(
        "quake_binary", "Quake binary location", String.class, new String(""));
    ParameterDefinition bitvectorpath = new ParameterDefinition(
        "bitvectorpath", "The path of bitvector ", String.class, new String(""));
    ParameterDefinition blockSize = new ParameterDefinition(
        "block_size", "block_size number of records are" + 
        "written to local files at a time.", Integer.class, new Integer(10000));
    for (ParameterDefinition def: new ParameterDefinition[] {quakeBinary, bitvectorpath, blockSize}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }
  
  public RunningJob runJob() throws Exception {
    JobConf conf = new JobConf(InvokeQuake.class);
    conf.setJobName("Quake correction");
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    String quakePath = (String) stage_options.get("quake_binary");
    String bitVectorPath = (String) stage_options.get("bitvectorpath");
    if(quakePath.length()== 0){
      throw new Exception("Please specify Quake path");
    }
    if(bitVectorPath.length()== 0){
      throw new Exception("Please specify bitvector path");
    }
    
    DistributedCache.addCacheFile(new Path(quakePath).toUri(),conf);
    DistributedCache.addCacheFile(new Path(bitVectorPath).toUri(),conf);

    // Sets the parameters in JobConf
    initializeJobConfiguration(conf);
    // The input could either be a MatePair (For quake correction in Mate Pairs)
    // or FastQRecord (For quake correction in Non Mate Pairs)
    // We create a union schema of these so that our mapper is able to accept
    // either schema as input.
    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(new FastQRecord().getSchema());
    schemas.add(new MatePair().getSchema());
    Schema unionSchema = Schema.createUnion(schemas);
    
    AvroJob.setMapperClass(conf, RunQuakeMapper.class);
    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    // Input  
    AvroJob.setInputSchema(conf, unionSchema);
    AvroJob.setOutputSchema(conf, new FastQRecord().getSchema());
    // Map Only Job
    conf.setNumReduceTasks(0);
    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }       
    long starttime = System.currentTimeMillis();            
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return result;  
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvokeQuake(), args);
    System.exit(res);
  }
}
