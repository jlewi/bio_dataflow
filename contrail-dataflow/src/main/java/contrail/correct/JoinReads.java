package contrail.correct;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

public class JoinReads extends MRStage {

  private static final Logger sLogger = Logger.getLogger(JoinReads.class);
  public static final Schema fast_q_record = (new FastQRecord()).getSchema();
  public static final Schema mate_record = (new MatePair()).getSchema();
  public static final Schema REDUCE_OUT_SCHEMA = mate_record;

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String,
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def : ContrailParameters
        .getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public static class JoinMapper extends AvroMapper <FastQRecord, Pair<CharSequence, FastQRecord>> {

    private static int K = 0;
    private Pair<CharSequence,FastQRecord> out_pair;

    private Pattern keyPattern;

    @Override
    public void configure(JobConf job) {
      out_pair = new Pair<CharSequence, FastQRecord>("",new FastQRecord());

      // Set the pattern to match the part of the read id that is common
      // to both reads in a mate pair.
      // TODO(jeremy@lewi.us): We should make this an argument.
      keyPattern = Pattern.compile("[^_/]*");
    }

  @Override
  public void map(FastQRecord record,
        AvroCollector<Pair<CharSequence, FastQRecord>> output, Reporter reporter)
            throws IOException {
      CharSequence key = record.getId();

      Matcher matcher = keyPattern.matcher(key);
      if (!matcher.find()) {
        reporter.incrCounter("Contrail", "Error-parsing-read-id", 1);
        return;
      }
      key = key.subSequence(matcher.start(), matcher.end());
      out_pair.set(key, record);
      output.collect(out_pair);
    }
  }

  public static class JoinReducer extends
  AvroReducer <CharSequence, FastQRecord, MatePair>
  {
    private MatePair joined;
    @Override
    public void configure(JobConf job)
    {
      joined = new MatePair();
    }
    @Override
    public void reduce(CharSequence id, Iterable<FastQRecord> iterable,
        AvroCollector<MatePair> collector, Reporter reporter)
            throws IOException {
      Iterator<FastQRecord> iter = iterable.iterator();
      FastQRecord mate_1 = iter.next();
      // We need to make a copy of the record because it will be overwritten
      // when we call next.
      mate_1 = SpecificData.get().deepCopy(
          mate_1.getSchema(), mate_1);
      if(!iter.hasNext()) {
        return;
      }
      FastQRecord mate_2 = iter.next();
      joined.left = mate_1;
      joined.right = mate_2;
      collector.collect(joined);
    }
  }

  @Override
  protected void setupConfHook() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    JobConf conf = (JobConf) getConf();
    FastQRecord read = new FastQRecord();

    Pair<CharSequence, FastQRecord> map_output =
        new Pair<CharSequence, FastQRecord>("", new FastQRecord());

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
    AvroJob.setOutputSchema(conf, JoinReads.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, JoinMapper.class);
    AvroJob.setReducerClass(conf, JoinReducer.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new JoinReads(), args);
    System.exit(res);
  }
}
