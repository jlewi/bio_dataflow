/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import java.io.IOException;
import java.util.ArrayList;
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
 * Output the contigs in TIGER/GDE contig format.
 *
 * Currently the contigs aren't aligned to the reads. To align the contigs
 * to the reads we would need to use the R5Tags and join the contigs with
 * the original reads. The R5Tags only contain the start location of
 * a read within a contig they don't indicate the overlap.
 *
 * See
 * https://sourceforge.net/apps/mediawiki/amos/index.php?title=Bank2contig
 * for a reference regarding the format.
 */
public class OutputContigs extends Stage {
  private static final Logger sLogger = Logger.getLogger(OutputContigs.class);

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
   * Mapper for converting the AVRO records into contig format.
   *
   * We use a regular mapper not an AVRO mapper because the output is not avro.
   */
  public static class GraphToFastqMapper extends MapReduceBase
      implements Mapper<AvroWrapper<GraphNodeData>,
                        NullWritable, Text, NullWritable > {
    private Text textOutput;
    private GraphNode node;
    private ArrayList<String> lines;
    public void configure(JobConf job) {
      textOutput = new Text();
      node = new GraphNode();
      lines = new ArrayList<String>();
    }

    public void map(AvroWrapper<GraphNodeData> nodeData, NullWritable inputValue,
        OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
      node.setData(nodeData.datum());
      lines.clear();

      // Header.
      // TODO(jeremy@lewi.us): We should align the contigs to the reads
      // using the R5Tags.
      lines.add(
          "##" + node.getNodeId() + " 0 " +
          node.getSequence().size() + " bases");
      lines.add(node.getSequence().toString());

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
    int res = ToolRunner.run(new Configuration(), new OutputContigs(), args);
    System.exit(res);
  }
}
