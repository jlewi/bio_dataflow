/**
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

/**
 * Convert the graph to fasta files.
 */
public class GraphToFasta extends MRStage {
  private static final Logger sLogger = Logger.getLogger(GraphToFasta.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
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
    @Override
    public void configure(JobConf job) {
      textOutput = new Text();
      node = new GraphNode();

      // Each entry in the FASTA file is 4 lines of text.
      lines = new String[2];
    }

    @Override
    public void map(AvroWrapper<GraphNodeData> nodeData, NullWritable inputValue,
        OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {
      node.setData(nodeData.datum());
      // In fasta format the id line begins with > sign.
      // Fastq uses @ to start the line.
      lines[0] = ">" + node.getNodeId();
      lines[1] = node.getSequence().toString();
      textOutput.set(StringUtils.join(lines, "\n"));
      output.collect(textOutput, NullWritable.get());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);

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
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphToFasta(), args);
    System.exit(res);
  }
}
