package contrail.correct;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

import contrail.sequences.FastQRecord;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/*
 * This is a simple class used to convert Normal Fastq Data into the Avro format fastq data.
 * Some of this is similar to FastqPreprocessorAvroCompressed, which outputs a compressed
 * form of fastQ reads. We, however, need uncompressed strings for preprocessing stages
 * for flash and quake. It would be inefficient to convert fastQ files into compressed Avro
 * fastQ files and reconvert it into uncompressed form for Flash/Quake processing. At the end of
 * preprocessing stages, we will have normal, corrected fastQ files which can be fed into
 * FastqPreprocessorAvroCompressed.
 *
 * This stage must be run separately for different kinds for reads, which are initially placed in
 * different inputPaths. This is because, each type of serves as input at a particular stage
 * of error correction. The different kinds of reads can be:
 *
 * 1. Unpaired reads (no flash, only quake)
 * 2. Paired Reads 1 for Flash
 * 3. Paired Reads 2 for Flash
 * 4. Paired Reads 1 for Quake (no flash)
 * 5. Paired Reads 2 for Quake (no flash)
 *
 * You can run this stage by specifying parameters in:
 *  --inputpath=path/to/input --outputpath=path/to/store/avro/converted/files
 *
 * TODO(jeremy@lewi.us): We should update or replace this class which uses
 * the new FastQ file input reader once that class is ready.
 */
public class FastQtoAvro extends Stage {

  public static class FastqPreprocessorMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, AvroWrapper<FastQRecord>, NullWritable> {
    private int idx = 0;

    private String fastqId = null;
    private String fastqRead = null;
    private String filename = null;
    private String fastqQvalue = null;


    private FastQRecord read = new FastQRecord();
    private AvroWrapper<FastQRecord> out_wrapper =
        new AvroWrapper<FastQRecord>(read);

    /*
     * The mapper takes in key value pairs in the normal format and emits out Avro data
     */
    public void map(
        LongWritable lineid, Text line,
        OutputCollector<AvroWrapper<FastQRecord>, NullWritable> output,
        Reporter reporter) throws IOException {
      if (idx == 0){
    	  fastqId = line.toString(); // The ID
      }
      else if (idx == 1) {
    	  fastqRead = line.toString(); // The kmer read
      }
      else if (idx == 2) {
    	  //plus sign - ignored
      }
      else if (idx == 3) {
    	  fastqQvalue = line.toString();
    	 /* Index of / is calculated since we need to make ids of mate pairs the same.
    	  *  We truncate everything after this /. This is required for the joining phases
    	  *  that follow. Two records (from mate pairs) with the same ID are joined for
    	  *  flash/quake run
    	  */
    	 int ind = fastqId.lastIndexOf('/');
     	 if (ind==-1){
     		/*fastq not in proper format for mate pair reading
     		 This condition can be handled explicitely if we want to
     		 preprocess only mate pairs, and data format is wrong somewhere
     		 for the mate pairs (ie. they miss /1 or /2)*/
     	 }

     	 fastqId = fastqId.substring(1,ind);
    	 read.setId(fastqId);
    	 read.setQvalue(fastqQvalue);
    	 read.setRead(fastqRead);

        output.collect(out_wrapper, NullWritable.get());
      }
      idx = (idx + 1) % 4;
    }

    public void close() throws IOException {
      if (idx != 0){
    	  // Non multiple of 4  split
        throw new IOException(
            "ERROR: closing with idx = " + idx + " in " + filename);
      }
    }
  }

  /**
   * Get the options required by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
	    HashMap<String, ParameterDefinition> defs =
	        new HashMap<String, ParameterDefinition>();

	    defs.putAll(super.createParameterDefinitions());

	    for (ParameterDefinition def:
	      ContrailParameters.getInputOutputPathOptions()) {
	      defs.put(def.getName(), def);
	    }
	    return Collections.unmodifiableMap(defs);
	  }

  @Override
  public RunningJob runJob() throws Exception {
    FastQRecord read = new FastQRecord();
    Schema OUT_SCHEMA = read.getSchema();
    String inputPath, outputPath;
    inputPath = (String) stage_options.get("inputpath");
    outputPath = (String) stage_options.get("outputpath");
    JobConf conf = new JobConf(FastQtoAvro.class);
    conf.setJobName("Conversion from text to Avro" + inputPath);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(FastqPreprocessorMapper.class);
    conf.setNumReduceTasks(0);

    conf.setInputFormat(NLineInputFormat.class);
    // linespermap must be a multiple of 4
    conf.setInt("mapred.line.input.format.linespermap", 2000000);

    AvroJob.setOutputSchema(conf,OUT_SCHEMA);

    //delete the output directory if it exists already
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
    int res = ToolRunner.run(new Configuration(), new FastQtoAvro(), args);
    System.exit(res);
  }
}