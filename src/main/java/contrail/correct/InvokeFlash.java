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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import contrail.util.ShellUtil;

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
    private String jobName;
    private ArrayList<String> fastqRecordsMateLeft;
    private ArrayList<String> fastqRecordsMateRight;
    private int count;
    private CorrectUtil correctUtil;
    private AvroCollector<FastQRecord> outputCollector;
    private int blockSize;
    @Override
    public void configure(JobConf job) {
      jobName = job.get("mapred.task.id");
      tempWritableFolder = FileHelper.createLocalTempDir().getAbsolutePath();
      //Initialise empty array lists
      fastqRecordsMateLeft = new ArrayList<String>();
      fastqRecordsMateRight = new ArrayList<String>();
      count = 0;
      outputCollector = null;
      correctUtil = new CorrectUtil();
      if (job.get("mapred.job.tracker").equals("local")) {
        // Local job runner doesn't support the distributed cache.
        // However in this case the flash binary should be local.
        InvokeFlash stage = new InvokeFlash();
        ParameterDefinition binaryDefinition =
            stage.getParameterDefinitions().get("flash_binary");
        sLogger.info(
            "Local job runner is being used. Distributed cache isn't " +
            "supported");
        flashHome = (String) binaryDefinition.parseJobConf(job);
        Path binaryPath = new Path(flashHome);
        String uriScheme = binaryPath.toUri().getScheme();
        if (uriScheme != null && !uriScheme.equals("file")) {
          sLogger.fatal(
              "If you are using the local job runner the flash binary should" +
              "be local but the URI was:" + uriScheme,
              new RuntimeException("Invalid URI scheme"));
          System.exit(-1);
        }
        // Get the path without the URI.
        flashHome = binaryPath.toUri().getPath();
      } else {
        // TODO(jeremy@lewi.us): We should get the basename of the flash
        // binary from the command line argument rather than hard coding it.
        flashHome = correctUtil.getDcachePath("flash", job);
      }
      InvokeFlash stage = new InvokeFlash();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      blockSize = (Integer)(definitions.get("block_size").parseJobConf(job));
      sLogger.info("Flash Home: " + flashHome);
    }

    @Override
    public void map(MatePair mateRecord,
        AvroCollector<FastQRecord> collector, Reporter reporter) throws IOException {
      if(outputCollector == null){
        outputCollector = collector;
      }
      count++;
      correctUtil.addMateToArrayLists(mateRecord, fastqRecordsMateLeft, fastqRecordsMateRight);
      // Time to process one block
      if(count == blockSize){
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
      correctUtil.writeLocalFile(fastqRecordsMateLeft,filePathFq1);
      correctUtil.writeLocalFile(fastqRecordsMateRight,filePathFq2);
      fastqRecordsMateLeft.clear();
      fastqRecordsMateRight.clear();
      ArrayList<String> command =  new ArrayList<String>();
      command.add(flashHome);
      command.add(filePathFq1);
      command.add(filePathFq2);
      command.add("-d");
      command.add(localOutFolderPath);/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      ShellUtil.execute(command, null, "flash", sLogger);
      String combinedFilePath = localOutFolderPath + "/out.extendedFrags.fastq";

      // collecting results of extended and not combined files
      correctUtil.emitFastqFileToHDFS(new File(combinedFilePath), collector);
      String notCombinedLeft = localOutFolderPath + "/out.notCombined_1.fastq";
      correctUtil.emitFastqFileToHDFS(new File(notCombinedLeft), collector);
      String notCombinedRight = localOutFolderPath + "/out.notCombined_2.fastq";
      correctUtil.emitFastqFileToHDFS(new File(notCombinedRight), collector);

      // Cleaning up the block Folder. The results of the extended file have been collected.
      tempFile = new File(localOutFolderPath);
      if(tempFile.exists()){
        FileUtils.deleteDirectory(tempFile);
      }
    }

    /**
     * Writes out the remaining chunk of data which is a non multiple of blockSize
     */
    @Override
    public void close() throws IOException{
      if(count > 0){
        runFlashOnInMemoryReads(outputCollector);
      }
      //delete the top level directory, and everything beneath
      File tempFile = new File(tempWritableFolder);
      if(tempFile.exists()){
        FileUtils.deleteDirectory(tempFile);
      }
    }
  }

  /* creates the custom definitions that we need for this phase*/
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition flashBinary = new ParameterDefinition(
    "flash_binary", "The URI of the flash binary. To use a filesystem other " +
    "than the default filesystem for hadoop you must specify an appropriate " +
    "scheme; e.g. file:/some/path/flash to use a local binary.",
    String.class, new String(""));
    ParameterDefinition blockSize = new ParameterDefinition(
        "block_size", "block_size number of records are" +
        " written to local files at a time.", Integer.class, new Integer(10000));
    for (ParameterDefinition def: new ParameterDefinition[] {flashBinary, blockSize}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  @Override
  public RunningJob runJob() throws Exception {
    logParameters();
    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("Flash invocation");
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    String flashBinary = (String) stage_options.get("flash_binary");
    if (flashBinary.length() == 0) {
      throw new Exception("Flash binary location required");
    }

    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (out_path.getFileSystem(conf).exists(out_path)) {
      out_path.getFileSystem(conf).delete(out_path, true);
    }

    // TODO: Distribute the flash binary to the workers. We need to copy the
    // binary to HDFS if the path isn't already on a distributed filesystem.
    // We should check if flash is in on the same filesystem as the input
    // or output paths. If it is then we can assume those are distributed
    // filesystems and just add the uri to the distributed cache. Otherwise
    // we need to copy the binary to the distributed filesystem. We can
    // just use a subdirectory e.g "dccache" of the outputpath.
    URI flashCacheURI = new Path(flashBinary).toUri();
    DistributedCache.addCacheFile(flashCacheURI, conf);

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

    long starttime = System.currentTimeMillis();
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) ((endtime - starttime) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return result;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvokeFlash(), args);
    System.exit(res);
  }
}