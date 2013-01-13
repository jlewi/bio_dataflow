package contrail.correct;
import org.apache.avro.Schema;

import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.stages.ParameterDefinition;
import contrail.stages.*;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
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

public class JoinReads extends Stage {

  private static final Logger sLogger = Logger.getLogger(JoinReads.class);
  public static final Schema fast_q_record = (new FastQRecord()).getSchema();
  public static final Schema mate_record = (new MatePair()).getSchema();

  public static final Schema REDUCE_OUT_SCHEMA = mate_record;


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

    public void configure(JobConf job)
    {
      out_pair = new Pair<CharSequence, FastQRecord>("",new FastQRecord());
    }

  public void map(FastQRecord record,
        AvroCollector<Pair<CharSequence, FastQRecord>> output, Reporter reporter)
            throws IOException {

      System.out.println("Test");
      CharSequence key = record.getId();
      System.out.println(key);
      out_pair.set(key, record);
      System.out.println(out_pair.toString());
      output.collect(out_pair);
    }
  }

  public static class JoinReducer extends
  AvroReducer <CharSequence, FastQRecord, MatePair>
  {
    public void reduce(CharSequence id, Iterable<FastQRecord> iterable,
        AvroCollector<MatePair> collector, Reporter reporter)
            throws IOException {

      Iterator<FastQRecord> iter = iterable.iterator();
      FastQRecord mate_1 = iter.next();
      // We need to make a copy of the record because it will be overwritten
      // when we call next.
      mate_1 = (FastQRecord) SpecificData.get().deepCopy(
          mate_1.getSchema(), mate_1);
      FastQRecord mate_2 = iter.next();
      mate_2 = iter.next();
      MatePair joined = new MatePair();
      joined.left = mate_1;
      joined.right = mate_2;
      System.out.println(mate_1.id + " " + mate_2.id);
      collector.collect(joined);
    }
  }

  public RunningJob runJob() throws Exception {

    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    System.out.println(inputPath + "+" +outputPath);
    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("Join Reads "+ inputPath);
    FastQRecord read = new FastQRecord();

    Pair<CharSequence, FastQRecord> map_output =
        new Pair<CharSequence, FastQRecord>("", new FastQRecord());

    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
    AvroJob.setOutputSchema(conf, JoinReads.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, JoinMapper.class);
    AvroJob.setReducerClass(conf, JoinReducer.class);

    if (stage_options.containsKey("writeconfig")) {
      writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already
      Path out_path = new Path(outputPath);
      if (FileSystem.get(conf).exists(out_path)) {
        // TODO(jlewi): We should only delete an existing directory
        // if explicitly told to do so.
        sLogger.info("Deleting output path: " + out_path.toString() + " " +
            "because it already exists.");
        FileSystem.get(conf).delete(out_path, true);
      }

      FileInputFormat.addInputPath(conf, new Path(inputPath));
      FileOutputFormat.setOutputPath(conf, new Path(outputPath));

      long starttime = System.currentTimeMillis();
      RunningJob result = JobClient.runJob(conf);

      long endtime = System.currentTimeMillis();
      float diff = (float) ((endtime - starttime) / 1000.0);

      sLogger.info("Runtime: " + diff + " s");
      return result;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new JoinReads(), args);
    System.exit(res);
  }
}
