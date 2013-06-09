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
import org.apache.commons.lang.StringUtils;
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
 * Rekey the graph nodes by component id.
 *
 * This MR takes two inputs:
 * 1. avro files containing GraphNodeData records and
 * 2. avro files containing Pair<key, List<String>>. The pairs assign
 * a key to a group of nodes. This MR job keys the GraphNodeData by
 * that id so the output is Pair<key, GraphNodeData>.
 * 3. If a node isn't assigned a key then its just keyed by its node id.
 */
public class RekeyByComponentId extends MRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(RekeyByComponentId.class);

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

  protected static Schema getInputSchema() {
    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(Pair.getPairSchema(
        Schema.create(Schema.Type.STRING),
        Schema.createArray(Schema.create(Schema.Type.STRING))));
    schemas.add(new GraphNodeData().getSchema());
    Schema union = Schema.createUnion(schemas);
    return union;
  }

  public static class Mapper extends
      AvroMapper<Object, Pair<CharSequence, Object>> {
    private Pair<CharSequence, Object> outPair;

    @Override
    public void configure(JobConf job) {
      outPair = new Pair<CharSequence, Object>(
          Schema.create(Schema.Type.STRING), getInputSchema());
    }

    @Override
    public void map(
        Object record,
        AvroCollector<Pair<CharSequence, Object>> collector, Reporter reporter)
            throws IOException {
      if (record instanceof GraphNodeData) {
        outPair.key(((GraphNodeData) record).getNodeId());
        outPair.value(record);
        collector.collect(outPair);
      } else {
        Pair<CharSequence, List<CharSequence>> component =
            (Pair<CharSequence, List<CharSequence>>) record;
        for (CharSequence nodeId : component.value()) {
          outPair.key(nodeId);
          outPair.value(component.key());
          collector.collect(outPair);
        }
      }
    }
  }

  public static class Reducer extends
      AvroReducer<CharSequence, Object, Pair<CharSequence, GraphNodeData>> {
    private Pair<CharSequence, GraphNodeData> outPair;
    private GraphNode node;
    private HashSet<String> keys;

    @Override
    public void configure(JobConf job) {
      outPair = new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
      node = new GraphNode();
      keys = new HashSet<String>();
    }

    @Override
    public void reduce(
        CharSequence id, Iterable<Object> records,
        AvroCollector<Pair<CharSequence, GraphNodeData>> collector,
        Reporter reporter) throws IOException {
      keys.clear();
      outPair.key(null);
      outPair.value(null);

      int numRecords = 0;
      int numKeys = 0;
      int numNodes = 0;

      for (Object record : records) {
        ++numRecords;
        if (record instanceof CharSequence) {
          String key = record.toString();
          outPair.key(key);
          keys.add(key);
          ++numKeys;
        } else {
          node.setData((GraphNodeData)record);
          outPair.value(node.clone().getData());
          ++numNodes;
        }
      }

      if (numKeys > 1) {
        sLogger.fatal(
            String.format(
                "Reduce key %s assigned multiple keys:%s", id,
                StringUtils.join(keys, ",")),
            new RuntimeException("Multiple keys"));
      }

      if (numNodes > 1) {
        sLogger.fatal(
            String.format(
                "Reduce key %s has multiple GraphNodeData records",
                id, StringUtils.join(keys, ",")),
            new RuntimeException("Multiple keys"));
      }
      if (numRecords > 2) {
        // There should be at most 2 records for each key.
        reporter.incrCounter("contrail", "error-more-than-2-records", 1);
        String outKey = "null";
        if (outPair.key() != null) {
          outKey = outPair.key().toString();
        }
        sLogger.fatal(
            "More than 2 records. Reduce Key:" + id + " out key:" + outKey);
      }

      if (outPair.key() != null && outPair.value() != null) {
        collector.collect(outPair);
      } else if (outPair.key() == null) {
        reporter.incrCounter("contrail", "unassigned-node", 1);
        // Set the key to the node id.
        // TODO(jeremy@lewi.us): This behavior, assigning a key of nodeId
        // to nodes not assigned a key was designed for resolving threads.
        // In ResolveThreadsPipeline we only assign threadable nodes and their
        // neighbors to groups.
        outPair.key(outPair.value().getNodeId());
        collector.collect(outPair);
      } else if (outPair.value() == null) {
        reporter.incrCounter("contrail", "error-missing-node-data", 1);
        sLogger.fatal(
            String.format("No GraphNodeData for node %s", id),
             new RuntimeException("Missing node data."));
      }
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    // Create the schema for the map output
    ArrayList<Schema> valueSchemas = new ArrayList<Schema>();
    valueSchemas.add(Schema.create(Schema.Type.STRING));
    valueSchemas.add(new GraphNodeData().getSchema());
    Schema valueSchema = Schema.createUnion(valueSchemas);

    AvroJob.setInputSchema(conf, getInputSchema());
    AvroJob.setMapOutputSchema(
        conf,  Pair.getPairSchema(Schema.create(Schema.Type.STRING),
            valueSchema));
    AvroJob.setOutputSchema(conf,
        Pair.getPairSchema(
            Schema.create(Schema.Type.STRING),
            new GraphNodeData().getSchema()));

    AvroJob.setMapperClass(conf, Mapper.class);
    AvroJob.setReducerClass(conf, Reducer.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new RekeyByComponentId(), args);
    System.exit(res);
  }
}
