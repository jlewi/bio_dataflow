package contrail.correct;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * This file converts an avro file of kmer count into a non Avro
 * <kmer, count> key value pair. This file will then be used for cutoff calculation
 */

public class ConverKMerCountsToText extends Stage{  
  /* Simply reads the file from HDFS and gives it to the reducer*/
  public static class copyPartMapper extends MapReduceBase implements Mapper <AvroWrapper<Pair<CharSequence, Long>>, NullWritable, Text, LongWritable> {

    @Override 
    public void map(AvroWrapper<Pair<CharSequence, Long>> key, NullWritable value,
        OutputCollector<Text, LongWritable> collector, Reporter reporter)
        throws IOException {
        Pair<CharSequence, Long> kmerCountPair = key.datum();
        collector.collect(new Text(kmerCountPair.key().toString()), new LongWritable(kmerCountPair.value()));
    }
  }
  
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
       defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }
  
  public RunningJob runJob() throws Exception {
    // Here the inputFile is not for a directory, but a specific path 
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
   
    JobConf conf = new JobConf(convertAvroFlashOutToFlatFastQ.class);
    Pair<CharSequence,Long> read = new Pair<CharSequence,Long>("", 0L);
    AvroJob.setInputSchema(conf, read.getSchema());
    conf.setJobName("Convert part file to non avro");
    AvroInputFormat<Pair<CharSequence,Long>> input_format =
        new AvroInputFormat<Pair<CharSequence,Long>>();
    conf.setInputFormat(input_format.getClass());
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    conf.setMapperClass(copyPartMapper.class);
    conf.setNumReduceTasks(0);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LongWritable.class);
    
    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
	  FileSystem.get(conf).delete(out_path, true);  
    }	
    long starttime = System.currentTimeMillis();            
    RunningJob runingJob = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return runingJob;
  }							
   
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ConverKMerCountsToText(), args);
    System.exit(res);
  }
}
