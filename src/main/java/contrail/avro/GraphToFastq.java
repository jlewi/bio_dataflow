//Author: Jeremy Lewi
package contrail.avro;

import contrail.CompressedRead;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.util.ByteReplaceAll;
import contrail.util.ByteUtil;

import java.io.IOException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class GraphToFastq extends Stage {
  private static final Logger sLogger = Logger.getLogger(GraphToFastq.class);


  /**
   * Mapper for converting the AVRO records into FASTQ format.
   *
   * We use a regular mapper not an AVRO mapper because the output is not avro.
   */
  public static class GraphToFastqMapper extends MapReduceBase
    implements Mapper<AvroWrapper<GraphNodeData>, NullWritable, Text, NullWritable > {
    private Text textOutput;
    private GraphNode node;
    public void configure(JobConf job) {
      textOutput = new Text();
      node = new GraphNode();
    }

   /**
    * Mapper.
    */
   @Override
   public void map(AvroWrapper<GraphNodeData> node, NullWritable inputValue,
       OutputCollector<Text, NullWritable> output, Reporter reporter)
           throws IOException {
     textOutput.set(node.getSequence().toString());
     output.collect(textOutput, NullWritable.get());
   }
 }


// @Override
// public RunningJob runJob() throws Exception {
//   String[] required_args = {"inputpath", "outputpath", "K"};
//   checkHasParametersOrDie(required_args);
//
//   String inputPath = (String) stage_options.get("inputpath");
//   String outputPath = (String) stage_options.get("outputpath");
//   int K = (Integer)stage_options.get("K");
//
//   sLogger.info(" - input: "  + inputPath);
//   sLogger.info(" - output: " + outputPath);
//
//   Configuration base_conf = getConf();
//   JobConf conf = null;
//   if (base_conf == null) {
//     conf = new JobConf(QuickMergeAvro.class);
//   } else {
//     conf = new JobConf(base_conf, QuickMergeAvro.class);
//   }
//   this.setConf(conf);
//
//   conf.setJobName("QuickMergeAvro " + inputPath + " " + K);
//
//   initializeJobConfiguration(conf);
//
//   FileInputFormat.addInputPath(conf, new Path(inputPath));
//   FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//   GraphNodeData graph_data = new GraphNodeData();
//   AvroJob.setInputSchema(conf, graph_data.getSchema());
//   AvroJob.setMapOutputSchema(conf, QuickMergeAvro.MAP_OUT_SCHEMA);
//   AvroJob.setOutputSchema(conf, QuickMergeAvro.REDUCE_OUT_SCHEMA);
//
//   AvroJob.setMapperClass(conf, QuickMergeMapper.class);
//   AvroJob.setReducerClass(conf, QuickMergeReducer.class);
//
//   if (stage_options.containsKey("writeconfig")) {
//     writeJobConfig(conf);
//   } else {
//     // Delete the output directory if it exists already
//     Path out_path = new Path(outputPath);
//     if (FileSystem.get(conf).exists(out_path)) {
//       // TODO(jlewi): We should only delete an existing directory
//       // if explicitly told to do so.
//       sLogger.info("Deleting output path: " + out_path.toString() + " " +
//           "because it already exists.");
//       FileSystem.get(conf).delete(out_path, true);
//     }
//
//     long starttime = System.currentTimeMillis();
//     RunningJob result = JobClient.runJob(conf);
//     long endtime = System.currentTimeMillis();
//
//     float diff = (float) ((endtime - starttime) / 1000.0);
//
//     System.out.println("Runtime: " + diff + " s");
//     return result;
//   }
//   return null;
// }
//
 public static void main(String[] args) throws Exception {
   int res = ToolRunner.run(new Configuration(), new GraphToFastq(), args);
   System.exit(res);
 }
}
