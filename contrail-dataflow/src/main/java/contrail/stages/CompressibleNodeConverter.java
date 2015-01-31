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

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphNodeData;


/**
 * A simple MR job to convert compressible nodes to graph nodes.
 *
 * This MR job takes as input a list of CompressibleNode records and outputs
 * the GraphNodeData stored in each record.
 *
 * Normally CompressChains finishes the non-distributed QuickMark and
 * QuickMerge which does the compression of the remaining nodes in one shot.
 * However in some cases, the distributed merge will fully compress the graph.
 * In this case we need to run an additional stage to extract the GraphNodes
 * from the data.
 *
 * This is a mapper only job.
 */
public class CompressibleNodeConverter extends MRStage     {
  private static final Logger sLogger = Logger.getLogger(
      CompressibleNodeConverter.class);
  public static final Schema REDUCE_OUT_SCHEMA =
      new GraphNodeData().getSchema();

  public static class ConverterMapper extends
      AvroMapper<CompressibleNodeData, GraphNodeData> {
    public void map(
        CompressibleNodeData compressibleNode,
        AvroCollector<GraphNodeData> collector,
        Reporter reporter) throws IOException {
      collector.collect(compressibleNode.getNode());
    }
  }
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

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    AvroJob.setInputSchema(conf, compressible_node.getSchema());

    AvroJob.setMapOutputSchema(
        conf,  CompressibleNodeConverter.REDUCE_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, CompressibleNodeConverter.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, ConverterMapper.class);

    // This is a mapper only job.
    conf.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CompressibleNodeConverter(), args);
    System.exit(res);
  }
}