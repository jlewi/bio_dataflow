// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.Node;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.NodeConverter;

/**
 * Mapreduce which converts a graph stored using the new avro serialization
 * format to the old serialization format.
 *
 * The job is a mapper only and the nodes are outputted in some random order.
 * This is done to save on the shuffle and reduce since most processing stages
 * order by different keys and will require resorting the graph anyway.
 */
public class ConvertAvroGraphToOldFormat extends Stage {
  private static final Logger sLogger = Logger.getLogger(QuickMergeAvro.class);

  private static class ConvertMapper extends MapReduceBase
  implements Mapper<AvroWrapper<GraphNodeData>, NullWritable, Text, Text> {
    /**
     * Mapper to do the conversion.
     */
    public void map(AvroWrapper<GraphNodeData> key, NullWritable bytes,
        OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
      GraphNodeData data = key.datum();
      GraphNode graph_node = new GraphNode(data);

      Node node = NodeConverter.graphNodeToNode(graph_node);
      output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
      reporter.incrCounter("Contrail", "nodes", 1);
   }
  }

  /**
   * Get the options required by this stage.
   */
  protected List<Option> getCommandLineOptions() {
    List<Option> options = super.getCommandLineOptions();

    options.addAll(ContrailOptions.getInputOutputPathOptions());
    return options;
  }

  protected void parseCommandLine(CommandLine line) {
    super.parseCommandLine(line);
    if (line.hasOption("inputpath")) {
      stage_options.put("inputpath", line.getOptionValue("inputpath"));
    }
    if (line.hasOption("outputpath")) {
      stage_options.put("outputpath", line.getOptionValue("outputpath"));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    sLogger.info("Tool name: ConvertAvroGraphToOldFormat");
    parseCommandLine(args);
    return run();
  }

  @Override
  protected int run() throws Exception {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - inputpath: "  + inputPath);
    sLogger.info(" - outputpath: " + outputPath);

    JobConf conf = new JobConf(ConvertAvroGraphToOldFormat.class);

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroInputFormat<GraphNodeData> input_format =
        new AvroInputFormat<GraphNodeData>();
    conf.setInputFormat(input_format.getClass());
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    // Make it mapper only.
    conf.setNumReduceTasks(0);
    conf.setMapperClass(ConvertMapper.class);

    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      // TODO(jlewi): We should only delete an existing directory
      // if explicitly told to do so.
      sLogger.info("Deleting output path: " + out_path.toString() + " " +
          "because it already exists.");
      FileSystem.get(conf).delete(out_path, true);
    }

    long starttime = System.currentTimeMillis();
    JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();

    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new ConvertAvroGraphToOldFormat(), args);
    System.exit(res);
  }
}
