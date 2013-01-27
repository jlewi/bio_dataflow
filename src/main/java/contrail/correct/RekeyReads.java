package contrail.correct;

import contrail.CompressedRead;

import contrail.sequences.CompressedSequence;
import contrail.sequences.FastQRecord;
import contrail.stages.*;
import contrail.io.FastQText;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Collections;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.commons.codec.binary.*;

/** MapReduce job to Rekey Data.
 * Before we actually do the merge, the data needs to be rekeyed in both the
 * input mate directories. The new keys should be in the "same" order in both
 * the datasets, and should preserve the 1-1 correspondence. The way we do this
 * is first rename all files in particular sorted order - this order is the
 * same for both input directories Next, we run a Map-Reduce job to re-key the
 * reads - as per the name of the file from which a particular read came, and
 * the way it is split.
 */

public class RekeyReads extends Stage {

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def : ContrailParameters
        .getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    ParameterDefinition type
    = new ParameterDefinition(
        "datatype",
        "The Data Type - either Singles (1), Mates for Flash (2), Mates Non Flash (3)",
        String.class, null);

    ParameterDefinition splitSize
    = new ParameterDefinition(
        "splitSize",
        "Size of the Input Split, required by the FastQInputFormat",
        Long.class, null);

    defs.put(type.getName(), type);
    defs.put(splitSize.getName(), splitSize);

    return Collections.unmodifiableMap(defs);
  }

  public static class RekeyMapper extends MapReduceBase implements
  Mapper<LongWritable, FastQText,  AvroWrapper<FastQRecord>, NullWritable> {
    private long                         count = 5;
    private String                      name        = null;
    private long                        splitNumber = 0;
    private String                      fqqvalue    = null;
    private String                      junk        = null;
    private String newKeyString = null;
    // List of files in the current Directory
    //private List<String>                files       = null;
    private static SortedSet<String> fileNameSet;
    private static List<String> fileNameList;
    // fileNumber holds the rank of this file from the sorted list of files in
    // this directory. Updated by getFileNumber()

    private String datatype;
    private long fileNumber  = 0;
    private FastQRecord read = new FastQRecord();
    private AvroWrapper<FastQRecord> out_wrapper =
        new AvroWrapper<FastQRecord>(read);

    /** Preprocessing in Configure Method.
     * Because we don't want to rename the files, we get the list of files in
     * the input directory, and sort the list. From the list, we get the rank of
     * current file from the sorted order. This operation needs to be only once
     * per mapper.
     */

    public void configure(JobConf conf) {

      String inputPath = conf.get("inputPath");
      FileStatus[] stats = null;
      FileSystem fs = null;
      SortedSet<String> fileNameSet = new TreeSet<String>();

      RekeyReads stage = new RekeyReads();

      datatype = conf.get("datatype");

      try {
        fs = FileSystem.get(conf);
        stats = fs.listStatus(new Path(inputPath));
        List<String> files = new ArrayList<String>(stats.length);
        for (FileStatus stat : stats) {
          String tempString = stat.getPath().toString();
          files.add(tempString.substring(tempString.lastIndexOf('/')+1));
        }

        if (datatype.equals("2") || datatype.equals("3"))
        {
          //TODO(deepak): Check if the files end in either _1.fq or _2.fq and
          // handle exceptions in a better way.
          // Also make sure that there are both _1 and _2 files.
          for (String file: files)
          {
            if ( file.endsWith("_1.fq") || file.endsWith("_2.fq"))
            {
              String absoluteName = file.substring(file.lastIndexOf('/')+1,
                  file.lastIndexOf("_"));
              if (absoluteName == null)
                continue;
              fileNameSet.add(absoluteName);
            }
          }
        }

        else
        {
          // else, for singles - simply add all filenames.
          for (String file: files)
          {
            String absoluteName = file.substring(file.lastIndexOf('/')+1);
            if (absoluteName == null)
              continue;
            fileNameSet.add(absoluteName);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
        throw new RuntimeException("fs.ListStatus Failed" + e.toString());
      }

      fileNameList = new ArrayList<String>(fileNameSet);
    }

    // The mapper takes in key value pairs in the normal format and emits out
    // Avro data

    public void map(LongWritable key, FastQText record,
        OutputCollector<AvroWrapper<FastQRecord> ,NullWritable> output,
        Reporter reporter) throws IOException {
      count = count +1 ;

      if (splitNumber == 0) {
        // get the filenumber. reporter object can return the current InputSplit
        // which contains the path to the current file.
        fileNumber = getFileNumber(reporter);
        splitNumber = getSplitNumber(reporter);
      }

      long newKey = getNewKey(fileNumber, splitNumber, count);
      newKeyString = Long.toString(newKey) + datatype;
      //System.out.println(record.getId() + " " + newKeyString);
      // TODO(dnettem): getbase64 encoding of this.
      read.id = newKeyString;
      read.read = record.getDna();
      read.qvalue = record.getQValue();

      output.collect(out_wrapper, NullWritable.get());
    }

    /* Get a base64 encoded String from a long type */

    public static String getBase64(long key)
    {
      byte[] byteArr = ByteBuffer.allocate(8).putLong(key).array();
      byte[] encoded = Base64.encodeBase64(byteArr);
      return new String(encoded);
    }

    /** Gets Split Number
     *
     * The method returns the rank of current file from the sorted order of
     * files
     */

    private static long getSplitNumber(Reporter reporter)
    {
      NumberedFileSplit nInpSplit = (NumberedFileSplit) reporter
          .getInputSplit();
      return nInpSplit.getNumber();
    }

    private static long getFileNumber(Reporter reporter){
      NumberedFileSplit nInpSplit = (NumberedFileSplit) reporter
          .getInputSplit();
      long fileNumber = 0;

      // This will give an index out of bounds exception of
      // the file name does not have _1 or _2 or is not a well formed path.
      String sourceFile = nInpSplit.getPath().toString();
      String absoluteName = sourceFile.substring(sourceFile.lastIndexOf('/')+1,
          sourceFile.lastIndexOf("_"));
      long i = fileNameList.indexOf(absoluteName);

      return i;
    }

    /** Generate new Key.
     *
     * The algorithm to generate the new key is to use fileNumber and
     * splitNumber as offset in a 64bit range.
     * We assume that records per file cannot be larger than 2^40.
     * For offset within the file. Since each split is capped at 256MB, assuming
     * each record to be 100 bytes which is < 5 * 10^5 records = 2^19.
     * Currently, we set the max number of lines per split to 2 * 10^6 which is
     * 5*10^5 records = 2^19. So each split won't have more than 2^20 records.
     * Using 64 bits gives us enough leeway to have 2^24 such files. This scheme
     * is wasteful. If the files are small, a lot of sequence ids will get wasted
     * This is a problem if we want to rekey the entire data.
     *
     * Example:
     * |-----24------|------20--------|-------20-------|
     *     FileNum        splitNum          count
     */

    private static long getNewKey(long fileNumber, long splitNumber, long count) {
      // splitNumber starts at 1.
      long newKey = (fileNumber << 40) + ((splitNumber-1) << 20) + count;
      return newKey;
    }
  }

  // Run Tool
  // /////////////////////////////////////////////////////////////////////////
  public RunningJob runJob() throws Exception {

    FastQRecord read = new FastQRecord();
    Schema OUT_SCHEMA = read.getSchema();
    String inputPath, outputPath, datatype;
    Long splitSize;
    JobConf conf = new JobConf(RekeyReads.class);
    conf.setJobName("Rekey Data");
    String[] required_args = {"inputpath", "outputpath","splitSize"};
    checkHasParametersOrDie(required_args);

    inputPath = (String) stage_options.get("inputpath");
    outputPath = (String) stage_options.get("outputpath");
    datatype = (String) stage_options.get("datatype");
    splitSize = (Long) stage_options.get("splitSize");

    //TODO (dnettem): Handle this better, but right now we know that there is
    // no singles data.
    if (datatype == null)
      datatype = "2";

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(FastQText.class);
    conf.setMapperClass(RekeyMapper.class);
    conf.setNumReduceTasks(0);
    conf.setInputFormat(FastQInputFormat.class);
    conf.setLong("FastQInputFormat.splitSize", splitSize);
    conf.setNumReduceTasks(0);
    // conf.setInt("mapred.line.input.format.linespermap", 200000); // must be a
    // multiple of 4
    conf.set("inputPath", inputPath);
    conf.set("datatype", datatype);
    AvroJob.setOutputSchema(conf, OUT_SCHEMA);
    // delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);
    }
    long start_time = System.currentTimeMillis();
    RunningJob result = JobClient.runJob(conf);
    long end_time = System.currentTimeMillis();
    double nseconds = (end_time - start_time) / 1000.0;
    System.out.println("Job took: " + nseconds + " seconds");
    return result;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RekeyReads(),
        args);
    System.exit(res);
  }
}

