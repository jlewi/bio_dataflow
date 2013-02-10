package contrail.correct;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * This file converts an avro file of kmer count into a non Avro
 * <kmer, count> key value pair. This file will then be used for cutoff c
 * calculation
 *
 */
public class ConvertKMerCountsToText extends Stage{
  /* Simply reads the file from HDFS and gives it to the reducer*/
  public static class ConvertMapper extends AvroMapper
      <Pair<CharSequence, Long>, Pair<CharSequence, Long>> {
    @Override
    public void map(
        Pair<CharSequence, Long> input,
        AvroCollector<Pair<CharSequence, Long>> collector,
        Reporter reporter) throws IOException {
        collector.collect(input);
    }
  }

  // We need a reducer to force the data to a single file.
  public static class ConvertReducer extends MapReduceBase implements
      Reducer<AvroWrapper<CharSequence>, AvroWrapper<Long>, Text, LongWritable> {
    private Text outKey;
    private LongWritable outValue;

    @Override
    public void configure(JobConf conf) {
      outKey = new Text();
      outValue = new LongWritable();
    }
    @Override
    public void reduce(
        AvroWrapper<CharSequence> key, Iterator<AvroWrapper<Long>> iter,
        OutputCollector<Text,LongWritable> collector, Reporter reporter)
            throws IOException {
      outKey.set(key.datum().toString());
      outValue.set(iter.next().datum());

      collector.collect(outKey, outValue);
      if (iter.hasNext()) {
        reporter.getCounter("contrail", "Error-multiple-counts-for-key");
      }
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
    logParameters();
    // Here the inputFile is not for a directory, but a specific path
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    JobConf conf = new JobConf(ConvertKMerCountsToText.class);
    Pair<CharSequence,Long> read = new Pair<CharSequence,Long>("", 0L);
    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setMapOutputSchema(
        conf, new Pair<CharSequence, Long>("", 0L).getSchema());
    conf.setJobName("Convert part file to non avro");
    AvroInputFormat<Pair<CharSequence,Long>> input_format =
        new AvroInputFormat<Pair<CharSequence,Long>>();
    conf.setInputFormat(input_format.getClass());
    conf.setOutputFormat(TextOutputFormat.class);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    AvroJob.setMapperClass(conf, ConvertMapper.class);

    // We use a single reducer because we want to force the data to a single
    // file.
    conf.setReducerClass(ConvertReducer.class);
    conf.setNumReduceTasks(1);
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
    float diff = (float) ((endtime - starttime) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return runingJob;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ConvertKMerCountsToText(), args);
    System.exit(res);
  }
}
