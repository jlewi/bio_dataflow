/*
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
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.CompressibleNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;
import contrail.graph.GraphNodeData;

/**
 * A simple mapreduce job which sorts the graph by the node ids.
 *
 * This program is useful if you want to build an avro sorted key value
 * file for the graph.
 */
public class SortGraph extends MRStage {
  private static final Logger sLogger = Logger.getLogger(SortGraph.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
      new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition parameter :
         ContrailParameters.getInputOutputPathOptions()) {
      defs.put(parameter.getName(), parameter);
    }

    return Collections.unmodifiableMap(defs);
  }

  public static class SortGraphMapper extends
    AvroMapper<Object, Pair<CharSequence, GraphNodeData>> {
    private Pair<CharSequence, GraphNodeData> pair;
    public void configure(JobConf job) {
      pair = new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
    }

    @Override
    public void map(Object input,
        AvroCollector<Pair<CharSequence, GraphNodeData>> collector,
        Reporter reporter)
            throws IOException {

      if (input instanceof GraphNodeData) {
        GraphNodeData nodeData = (GraphNodeData) input;
        pair.set(nodeData.getNodeId(), nodeData);
      } else if (input instanceof CompressibleNodeData) {
        CompressibleNodeData compressibleNode = (CompressibleNodeData) input;
        pair.set(
            compressibleNode.getNode().getNodeId(), compressibleNode.getNode());
      }
      collector.collect(pair);
    }
  }

  public static class SortGraphReducer extends
      AvroReducer<CharSequence, GraphNodeData, GraphNodeData> {
    @Override
    public void reduce(CharSequence nodeId, Iterable<GraphNodeData> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter)
            throws IOException {
      Iterator<GraphNodeData> iterator = iterable.iterator();
      if (!iterator.hasNext()) {
        sLogger.fatal(
            "No node for nodeid:" + nodeId, new RuntimeException("No node."));
        throw new RuntimeException("No node.");
      }
      collector.collect(iterator.next());
      if (iterator.hasNext()) {
        sLogger.fatal(
            "Multiple nodes for nodeid:" + nodeId,
            new RuntimeException("multiple nodes."));
        throw new RuntimeException("Multiple node.");
      }
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) (getConf());

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    GraphNodeData nodeData = new GraphNodeData();
    CompressibleNodeData compressibleData = new CompressibleNodeData();

    // We need to create a schema representing the union of GraphNodeData
    // and CompressibleNodeData.
    schemas.add(nodeData.getSchema());
    schemas.add(compressibleData.getSchema());
    Schema unionSchema = Schema.createUnion(schemas);

    AvroJob.setInputSchema(conf, unionSchema);

    Pair<CharSequence, GraphNodeData> pair =
        new Pair<CharSequence, GraphNodeData>("", nodeData);
    AvroJob.setMapOutputSchema(conf, pair.getSchema());
    AvroJob.setOutputSchema(conf, nodeData.getSchema());

    AvroJob.setMapperClass(conf, SortGraphMapper.class);
    AvroJob.setReducerClass(conf, SortGraphReducer.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortGraph(), args);
    System.exit(res);
  }
}
