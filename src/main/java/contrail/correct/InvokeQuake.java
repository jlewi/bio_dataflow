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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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
import contrail.util.ShellUtil;


/**
 * This stage uses quake to correct fastq reads.
 *
 * This class deals with correction of both
 * paired reads (MatePair) and unpaired reads (FastQRecord).
 * The input to this class is an AVRO file containing either FastQRecoords or
 * MatePair records (which essentially contains two fastQ records).
 *
 * Assumptions:
 * 1. Quake binary is available, and its path specified in the parameters. This is used
 *     later to load the binary from the location into Distributed Cache
 * 2. We have access to a system temp directoy on each node; i.e the java
 *   command File.createTempFile succeeds.
 * 3. The bitvector of kmers above the cutoff has been constructed and is
 *   available at a location specified in the input
 *
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
 *
 * TODO(jeremy@lewi.us): Should we add an option to use quake's ability
 * to handle paired reads differently from unpaired reads? We would need
 * to restructure the code or pipeline so that the input only consisted
 * of single or paired reads.
 */
public class InvokeQuake extends Stage{
  private static final Logger sLogger = Logger.getLogger(InvokeQuake.class);

  public static class RunQuakeMapper extends AvroMapper<Object, FastQRecord>{
    private String quakeHome = null;
    private String bitVectorPath = null;
    // fastqRecordList is used in case the input AVRO file follows FastQRecord schema
    private ArrayList<String> fastqRecordList;
    private int K;

    private int count;
    private int blockSize;
    private CorrectUtil correctUtil;
    private AvroCollector<FastQRecord> outputCollector;
    private Reporter reporter;

    // Keeps track of how many blocks of reads we have processed.
    private int block;

    @Override
    public void configure(JobConf job) {
      InvokeQuake stage = new InvokeQuake();

      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));

      blockSize = (Integer)(definitions.get("block_size").parseJobConf(job));
      sLogger.info("blockSize "+ blockSize);
      correctUtil = new CorrectUtil();

      // Initialize empty array list
      fastqRecordList= new ArrayList<String>();
      count = 0;
      outputCollector = null;
      reporter = null;
      block = 0;

      ParameterDefinition binaryDefinition =
          stage.getParameterDefinitions().get("quake_binary");
      ParameterDefinition vectorDefinition =
          stage.getParameterDefinitions().get("bitvectorpath");
      quakeHome = (String) binaryDefinition.parseJobConf(job);
      bitVectorPath = (String) vectorDefinition.parseJobConf(job);

      // Get the necessary files from the distributed cache.
      if (job.get("mapred.job.tracker").equals("local")) {
        // Local job runner doesn't support the distributed cache.
        // However in this case the files will be local.
        sLogger.info(
            "Local job runner is being used. Distributed cache isn't " +
            "supported");

        Path binaryPath = new Path(quakeHome);
        String uriScheme = binaryPath.toUri().getScheme();
        if (uriScheme != null && !uriScheme.equals("file")) {
          sLogger.fatal(
              "If you are using the local job runner the quake binary should" +
              "be local but the URI was:" + uriScheme,
              new RuntimeException("Invalid URI scheme"));
          System.exit(-1);
        }
        // Get the path without the URI.
        quakeHome = binaryPath.toUri().getPath();

        Path vectorPath = new Path(bitVectorPath);
        uriScheme = vectorPath.toUri().getScheme();
        if (uriScheme != null && !uriScheme.equals("file")) {
          sLogger.fatal(
              "If you are using the local job runner the bitvectorpath should" +
              "be local but the URI was:" + uriScheme,
              new RuntimeException("Invalid URI scheme"));
          System.exit(-1);
        }
        // Get the path without the URI.
        bitVectorPath = vectorPath.toUri().getPath();
      } else {
        // Get the base names so we can look them.
        String quakeBase = FilenameUtils.getName(quakeHome);
        String bitBase = FilenameUtils.getName(bitVectorPath);

        quakeHome = correctUtil.getDcachePath(quakeBase, job);
        bitVectorPath = correctUtil.getDcachePath(bitBase, job);
      }

      if(quakeHome.length() == 0){
        sLogger.fatal("The path to the quake binary could not be retrieved.");
        System.exit(-1);
      }

      if(bitVectorPath.length() == 0){
        sLogger.fatal("The path to the bitvector count not be retrieved.");
        System.exit(-1);
      }
      sLogger.info("Quake Home: " + quakeHome);
      sLogger.info("BitVector Location: " + bitVectorPath);
    }

    public void map(
        Object record, AvroCollector<FastQRecord> collector, Reporter reporter)
            throws IOException {
      if(record instanceof FastQRecord){
        ++count;
        FastQRecord fastqRecord = (FastQRecord)record;
        fastqRecordList.add(correctUtil.fastqRecordToString(fastqRecord));
        reporter.incrCounter("contrail", "input-reads", 1);
      }

      if(record instanceof MatePair){
        MatePair mateRecord = (MatePair)record;
        fastqRecordList.add(correctUtil.fastqRecordToString(
            mateRecord.getLeft()));
        fastqRecordList.add(correctUtil.fastqRecordToString(
            mateRecord.getRight()));
        count += 2;
        reporter.incrCounter("contrail", "input-reads", 2);
      }

      if(outputCollector == null){
        outputCollector = collector;
        this.reporter = reporter;
      }

      // Time to process one block
      if(count == blockSize){
        runQuakeOnInMemoryReads(collector, reporter);
        count = 0;
      }
    }

    /**
     * This method runs quake locally and collects the results.
     * @param output: The reference of the collector
     * @throws IOException
     */
    private void runQuakeOnInMemoryReads(
        AvroCollector<FastQRecord> output, Reporter reporter)
            throws IOException {
      // Create a directory for this block of reads.
      // Hadoop should set the temporary directory to a unique directory for
      // each task attempt so we shouldn't need to worry about two tasks
      // trying to use the same directory. We will delete blockDir but
      // rely on hadoop to clean up the toplevel temporary directory.
      File blockDir = new File(FilenameUtils.concat(
          FileUtils.getTempDirectory().getPath(),
          String.format("block_%05d", block)));
      if (!blockDir.mkdirs()) {
        sLogger.fatal(
            "Couldn't create the directory:" + blockDir.getPath(),
            new RuntimeException("Couldn't create directory."));
        System.exit(-1);
      }

      String fastqPath = FilenameUtils.concat(
          blockDir.getPath(), "fastq_records.fq");
      correctUtil.writeLocalFile(fastqRecordList, fastqPath);

      // Correction command
      ArrayList<String> command = new ArrayList<String>();
      command.add(quakeHome);
      command.add("-r");
      command.add(fastqPath);
      command.add("-k");
      command.add(Integer.toString(K));
      command.add("-b");
      command.add(bitVectorPath);
      // The headers option prevents quake info from being included in
      // the output read id.
      command.add("--headers");
      if (ShellUtil.execute(command, blockDir.getPath(), "quake", sLogger) !=
          0) {
        sLogger.fatal(
            "Quake didn't run successfully",
            new RuntimeException("Quake failed"));
        System.exit(-1);
      }

      File correctedFilePath = new File(
          FilenameUtils.removeExtension(fastqPath) +  ".cor.fq");

      if (!correctedFilePath.exists()) {
        sLogger.fatal(
            "Expected the corrected reads to be in file:" +
            correctedFilePath.getPath() + " but the file doesn't exist.",
            new RuntimeException("Problem with quake"));
      }
      sLogger.info("corrected path: " + correctedFilePath.getPath());
      correctUtil.emitFastqFileToHDFS(correctedFilePath, output);

      // Read the stats.
      File statsPath = new File(
          FilenameUtils.removeExtension(fastqPath) +  ".stats.txt");
      FileReader fstream = new FileReader(statsPath);
      BufferedReader fileReader = new BufferedReader(fstream);
      String line;
      while((line= fileReader.readLine()) != null){
        String[] pieces = line.split(":", 2);
        if (pieces.length != 2) {
          reporter.incrCounter("contrail", "error-parsing-stats-line", 1);
          continue;
        }
        reporter.incrCounter(
            "contrail", "quake-reads-" + pieces[0].toLowerCase(),
            Integer.parseInt(pieces[1].trim()));
      }
      fileReader.close();
      fstream.close();


      // File.Delete won't work because the directory isn't empty.
      try {
        FileUtils.deleteDirectory(blockDir);
      } catch (IOException e) {
        sLogger.fatal("There was a problem deleting:" + blockDir.getPath(), e);
        System.exit(-1);
      }

      fastqRecordList.clear();
      ++block;
    }

    /**
     * Writes out the remaining chunk of data which is a non multiple of blockSize
     */
    public void close() throws IOException{
      if(count > 0) {
        runQuakeOnInMemoryReads(outputCollector, reporter);
      }
    }
  }

  /**
   *  creates the custom definitions that we need for this phase
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition quakeBinary = new ParameterDefinition(
        "quake_binary", "The path to the correct binary in quake.",
        String.class, null);
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
    // Check for missing arguments.
    String[] required_args = {
        "inputpath", "outputpath", "quake_binary", "bitvectorpath"};
    checkHasParametersOrDie(required_args);
    logParameters();
    // TODO(jeremy@lewi.us): We should initialize the conf based on getConf().
    JobConf conf = new JobConf(InvokeQuake.class);
    conf.setJobName("Quake correction");
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    String quakePath = (String) stage_options.get("quake_binary");
    String bitVectorPath = (String) stage_options.get("bitvectorpath");
    if(quakePath.length() == 0){
      throw new Exception("Please specify Quake path");
    }
    if(bitVectorPath.length()== 0){
      throw new Exception("Please specify bitvector path");
    }

    // TODO: We should check if the files aren't on HDFS and if they aren't
    // we should copy the files to HDFS.
    DistributedCache.addCacheFile(new Path(quakePath).toUri(), conf);
    DistributedCache.addCacheFile(new Path(bitVectorPath).toUri(), conf);

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
    float diff = (float) ((endtime - starttime) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return result;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InvokeQuake(), args);
    System.exit(res);
  }
}
