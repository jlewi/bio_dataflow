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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.util.ContrailLogger;

/**
 * Group nodes according to some id. The input are pairs (key, GraphNodeData).
 * The nodes are grouped by key and outputed as a list of nodes, one
 * list per key.
 */
public class GroupByComponentId extends MRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(GroupByComponentId.class);

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

  public static class Mapper extends
      AvroMapper<Pair<CharSequence, GraphNodeData>,
                 Pair<CharSequence, GraphNodeData>> {
    @Override
    public void configure(JobConf job) {
      ArrayList<Schema> schemas = new ArrayList<Schema>();
      schemas.add(Schema.create(Schema.Type.STRING));
      schemas.add(new GraphNodeData().getSchema());
      Schema union = Schema.createUnion(schemas);
    }

    @Override
    public void map(
        Pair<CharSequence, GraphNodeData> record,
        AvroCollector<Pair<CharSequence, GraphNodeData>> collector,
        Reporter reporter) throws IOException {
      collector.collect(record);
    }
  }

  public static class Reducer extends
      AvroReducer<CharSequence, GraphNodeData, List<GraphNodeData>> {
    private List<GraphNodeData> out;
    private GraphNode node;
    private HashSet<String> nodeIds;

    @Override
    public void configure(JobConf job) {
      out = new ArrayList<GraphNodeData>();
      node = new GraphNode();
      nodeIds = new HashSet<String>();
    }

    @Override
    public void reduce(
        CharSequence id, Iterable<GraphNodeData> records,
        AvroCollector<List<GraphNodeData>> collector,
        Reporter reporter) throws IOException {
      out.clear();
      nodeIds.clear();
      for (GraphNodeData nodeData : records) {
        node.setData(nodeData);
        if (nodeIds.contains(node.getNodeId())) {
          sLogger.fatal(String.format(
              "node:%s appears multiple times in group:%s", node.getNodeId(),
              id), new RuntimeException("Invalid input."));
        }
        nodeIds.add(node.getNodeId());
        out.add(node.clone().getData());
      }
      collector.collect(out);
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    Schema pairSchema = Pair.getPairSchema(
        Schema.create(Schema.Type.STRING),
        new GraphNodeData().getSchema());

    AvroJob.setInputSchema(conf, pairSchema);
    AvroJob.setMapOutputSchema(conf,  pairSchema);
    AvroJob.setOutputSchema(
        conf, Schema.createArray(new GraphNodeData().getSchema()));

    AvroJob.setMapperClass(conf, Mapper.class);
    AvroJob.setReducerClass(conf, Reducer.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new GroupByComponentId(), args);
    System.exit(res);
  }
}
