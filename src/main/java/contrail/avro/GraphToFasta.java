//Author: Jeremy Lewi
package contrail.avro;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang.StringUtils;
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
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Convert the graph to fasta files.
 */
public class GraphToFasta extends Stage {
  private static final Logger sLogger = Logger.getLogger(GraphToFasta.class);

  /**
   * Get the parameters used by this stage.
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

  /**
   * Mapper for converting the AVRO records into FASTQ format.
   *
   * We use a regular mapper not an AVRO mapper because the output is not avro.
   */
  public static class GraphToFastqMapper extends MapReduceBase
  implements Mapper<AvroWrapper<GraphNodeData>, NullWritable, Text, NullWritable > {
    private Text textOutput;
    private GraphNode node;
    private String[] lines;
    public void configure(JobConf job) {
      textOutput = new Text();
      node = new GraphNode();

      // Each entry in the FASTA file is 4 lines of text.
      lines = new String[2];
    }

    public void map(AvroWrapper<GraphNodeData> nodeData, NullWritable inputValue,
        OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
      node.setData(nodeData.datum());
      lines[0] = "@" + node.getNodeId();
      lines[1] = node.getSequence().toString();
      textOutput.set(StringUtils.join(lines, "\n"));
      output.collect(textOutput, NullWritable.get());
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - inputpath: "  + inputPath);
    sLogger.info(" - outputpath: " + outputPath);

    JobConf conf = new JobConf(GraphToFasta.class);

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
    conf.setMapperClass(GraphToFastqMapper.class);

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

      long starttime = System.currentTimeMillis();
      RunningJob result = JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);

      System.out.println("Runtime: " + diff + " s");
      return result;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphToFasta(), args);
    System.exit(res);
  }
}
