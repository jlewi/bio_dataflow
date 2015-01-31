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
// Author: Michael Schatz, Jeremy Lewi
package contrail.stages;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.stages.GraphCounters.CounterName;

public class QuickMergeAvro extends MRStage {
  private static final Logger sLogger = Logger.getLogger(QuickMergeAvro.class);

  /**
   * Define the schema for the mapper output. The keys will be a string
   * containing the id for the node. The value will be an instance of
   * GraphNodeData.
   */
  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(
          Schema.create(Schema.Type.STRING), (new GraphNodeData()).getSchema());

  public final static CounterName NUM_COMPRESSED_CHAINS =
      new CounterName("contrail", "num-compressed-chains");
  public final static CounterName NUM_COMPRESSED_NODES =
      new CounterName("contrail", "num-compressed-nodes-in-chains");

  /**
   * Define the schema for the reducer output. The keys will be a byte buffer
   * representing the compressed source KMer sequence. The value will be an
   * instance of GraphNodeData.
   */
  public static final Schema REDUCE_OUT_SCHEMA =
      new GraphNodeData().getSchema();

  public static String KMerTag(GraphNodeData data) {
    // The key is the read tag along with the chunk.
    // We want to group KMers coming from the same read as they are likely
    // to form linear chains. We need to use the chunk as well because
    // the chunk segments tags based on repeat KMers.
    // TODO(jeremy@lewi.us): We should probably be using
    // KMerReadTag.toString();
    String mertag = data.getMertag().getReadTag().toString() + "_" +
        data.getMertag().getChunk();
    return mertag;
  }

  public static class QuickMergeMapper extends
      AvroMapper<GraphNodeData, Pair<CharSequence, GraphNodeData>> {

    private Pair<CharSequence, GraphNodeData> out_pair;

    @Override
    public void configure(JobConf job) {
      out_pair = new Pair<CharSequence, GraphNodeData>(MAP_OUT_SCHEMA);
    }

    /**
     * Mapper for QuickMerge.
     *
     * Input is an avro file containing the nodes for the graph.
     * For each input, we output the GraphNodeData keyed by the mertag so as to
     * group nodes coming from the same read.
     */
    @Override
    public void map(
        GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, GraphNodeData>> output,
        Reporter reporter) throws IOException {
      String mertag = KMerTag(graph_data);

      out_pair.set(mertag, graph_data);
      output.collect(out_pair);
      reporter.incrCounter("Contrail", "nodes", 1);
    }
  }

  /**
   * In the reducer we try to merge nodes forming linear chains.
   *
   * By construction, most of the nodes coming from a given read will form a
   * linear chain. We do the merge by using the mapper to group nodes using the
   * read tag and chunk. This heuristic has a good chance of grouping together
   * nodes which can be merged because if K is << the read length we will
   * get many edges from each read that can most likely be merged together.
   */
  public static class QuickMergeReducer extends
      AvroReducer<CharSequence, GraphNodeData, GraphNodeData> {
    private static int K = 0;
    public static boolean VERBOSE = false;

    @Override
    public void configure(JobConf job) {
      QuickMergeAvro stage = new QuickMergeAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));
    }

    /**
     * Reducer for QuickMerge.
     */
    @Override
    public void reduce(CharSequence  mertag, Iterable<GraphNodeData> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter)
            throws IOException {
      // The number of compressed chains.
      int num_compressed_chains  = 0;
      // Total number of nodes used to form the compressed chains.
      int num_nodes_in_compressed_chains = 0;

      // Load the nodes into memory.
      Map<String, GraphNode> nodes = new HashMap<String, GraphNode>();
      Iterator<GraphNodeData> iter = iterable.iterator();

      while(iter.hasNext()) {
        // We need to make a copy of GraphNodeData because iterable
        // will reuse the same instance when next is called.
        GraphNodeData value = iter.next();
        GraphNode node = new GraphNode(value);
        node = node.clone();
        nodes.put(node.getNodeId().toString(), node);
      }

      // Create a list of the nodes to process. We need to make a copy of
      // nodes.keySet otherwise when we remove an entry from the set we remove
      // it from the hashtable.
      Set<String> nodes_to_process = new HashSet<String>();
      nodes_to_process.addAll(nodes.keySet());

      while (nodes_to_process.size() > 0) {
        String nodeid = nodes_to_process.iterator().next();
        nodes_to_process.remove(nodeid);

        GraphNode start_node = nodes.get(nodeid);

        if (start_node == null) {
          throw new RuntimeException("Start node shouldn't be null");
        }

        // Find a chain if any to merge.
        QuickMergeUtil.NodesToMerge nodes_to_merge =
            QuickMergeUtil.findNodesToMerge(nodes, start_node);

        // Remove all the nodes visited from the list of ids to process.
        nodes_to_process.removeAll(nodes_to_merge.nodeids_visited);

        if (nodes_to_merge.start_terminal == null &&
            nodes_to_merge.end_terminal == null) {
          continue;
        }

        // Merge the nodes.
        QuickMergeUtil.ChainMergeResult merge_result =
            QuickMergeUtil.mergeLinearChain(nodes, nodes_to_merge, K - 1);

        num_compressed_chains += 1;
        num_nodes_in_compressed_chains += merge_result.merged_nodeids.size();

        // Remove the merged nodes from nodes because these should not
        // be outputted.
        for (String merged_nodeid: merge_result.merged_nodeids){
          nodes.remove(merged_nodeid);
        }

        // Add the newly merged node to the list of nodes.
        nodes.put(merge_result.merged_node.getNodeId(),
            merge_result.merged_node);
      }

      // Output all the remaining nodes.
      for(String nodeid : nodes.keySet()) {
        collector.collect(nodes.get(nodeid).getData());
      }

      reporter.incrCounter(
          NUM_COMPRESSED_CHAINS.group, NUM_COMPRESSED_CHAINS.tag,
          num_compressed_chains);
      reporter.incrCounter(
          NUM_COMPRESSED_NODES.group, NUM_COMPRESSED_NODES.tag,
          num_nodes_in_compressed_chains);
    }
  }

  @Override
  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);
    return defs;
  }

  @Override
  protected void setupConfHook() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    JobConf conf = (JobConf) getConf();
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, QuickMergeAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, QuickMergeAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, QuickMergeMapper.class);
    AvroJob.setReducerClass(conf, QuickMergeReducer.class);
  }

  @Override
  protected void postRunHook() {
    try {
      long numCompressedChains = job.getCounters().findCounter(
          NUM_COMPRESSED_CHAINS.group,
          NUM_COMPRESSED_CHAINS.tag).getValue();
      long numCompressedNodes = job.getCounters().findCounter(
          NUM_COMPRESSED_NODES.group,
          NUM_COMPRESSED_NODES.tag).getValue();

      long numNodes = job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "REDUCE_OUTPUT_RECORDS").getValue();
      sLogger.info("Number of chains compressed:" + numCompressedChains);
      sLogger.info("Number of nodes compressed:" + numCompressedNodes);
      sLogger.info("Number of nodes outputed:" + numNodes);
    } catch (IOException e) {
      sLogger.fatal("Couldn't get counters", e);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new QuickMergeAvro(), args);
    System.exit(res);
  }
}
