package contrail.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import contrail.stages.NotImplementedException;
import contrail.graph.GraphNodeData;
import contrail.graph.KMerEdge;

// TOOD(jlewi): THIS IS ALL INCOMPLETE CODE
/**
 * A simple mapreduce job which sorts the graph by the node ids.
 */
public class SortGraph {
  private static final Logger sLogger = Logger.getLogger(SortGraph.class);


  public static final Schema graph_node_data_schema =
      (new GraphNodeData()).getSchema();
  /**
   * Define the schema for the mapper output. The keys will be a byte buffer
   * representing the compressed source KMer sequence. The value will be an
   * instance of KMerEdge.
   */
  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(
          Schema.create(Schema.Type.STRING), graph_node_data_schema);

  /**
   * Define the schema for the reducer output.
   */
  public static final Schema REDUCE_OUT_SCHEMA = graph_node_data_schema;

  public static class SortGraphMapper extends
  AvroMapper<GraphNodeData, Pair<CharSequence, GraphNodeData>>
  {
    private Pair<CharSequence, GraphNodeData> pair;
    public void configure(JobConf job)
    {
      pair = new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
    }

    @Override
    public void map(GraphNodeData input,
        AvroCollector<Pair<CharSequence, GraphNodeData>> output, Reporter reporter)
            throws IOException {
      throw new NotImplementedException("Need to write the code");
    }
  }

  public static class SortGraphReducer extends
  AvroReducer<ByteBuffer, KMerEdge, GraphNodeData> {
    public void configure(JobConf job) {
      SortGraph stage = new SortGraph();
    }

    @Override
    public void reduce(ByteBuffer source_kmer_packed_bytes, Iterable<KMerEdge> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter)
            throws IOException {
      throw new NotImplementedException("Need to write the code");
    }
  }

//  @Override
//  public RunningJob runJob() throws Exception {
//    // Check for missing arguments.
//    String[] required_args = {"inputpath", "outputpath", "K"};
//    checkHasParametersOrDie(required_args);
//
//    String inputPath = (String) stage_options.get("inputpath");
//    String outputPath = (String) stage_options.get("outputpath");
//    int K = (Integer)stage_options.get("K");
//
//    sLogger.info(" - input: "  + inputPath);
//    sLogger.info(" - output: " + outputPath);
//
//    Configuration base_conf = getConf();
//    JobConf conf = null;
//    if (base_conf != null) {
//      conf = new JobConf(getConf(), this.getClass());
//    } else {
//      conf = new JobConf(this.getClass());
//    }
//    conf.setJobName("BuildGraph " + inputPath + " " + K);
//
//    initializeJobConfiguration(conf);
//
//    FileInputFormat.addInputPath(conf, new Path(inputPath));
//    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//    CompressedRead read = new CompressedRead();
//    AvroJob.setInputSchema(conf, read.getSchema());
//    AvroJob.setMapOutputSchema(conf, BuildGraphAvro.MAP_OUT_SCHEMA);
//    AvroJob.setOutputSchema(conf, BuildGraphAvro.REDUCE_OUT_SCHEMA);
//
//    AvroJob.setMapperClass(conf, BuildGraphMapper.class);
//    AvroJob.setReducerClass(conf, BuildGraphReducer.class);
//
//    if (stage_options.containsKey("writeconfig")) {
//      writeJobConfig(conf);
//    } else {
//      // Delete the output directory if it exists already
//      Path out_path = new Path(outputPath);
//      if (FileSystem.get(conf).exists(out_path)) {
//        // TODO(jlewi): We should only delete an existing directory
//        // if explicitly told to do so.
//        sLogger.info("Deleting output path: " + out_path.toString() + " " +
//            "because it already exists.");
//        FileSystem.get(conf).delete(out_path, true);
//      }
//
//      long starttime = System.currentTimeMillis();
//      RunningJob result = JobClient.runJob(conf);
//      long endtime = System.currentTimeMillis();
//
//      float diff = (float) ((endtime - starttime) / 1000.0);
//
//      sLogger.info("Runtime: " + diff + " s");
//      return result;
//    }
//    return null;
//  }
//
//  public static void main(String[] args) throws Exception {
//    int res = ToolRunner.run(new Configuration(), new SortGraph(), args);
//    System.exit(res);
//  }
}
