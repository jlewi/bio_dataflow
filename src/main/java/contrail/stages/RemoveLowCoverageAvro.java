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
package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.log4j.Logger;

import contrail.RemoveNeighborMessage;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.NeighborData;
import contrail.sequences.DNAStrand;
import contrail.stages.GraphCounters.CounterName;

/**
 * RemoveLowCoverage is the last phase in correcting errors;
 * In mapper we look over the lengths of the Sequences and
 * their respective coverage; and check if they are above a threshold;
 * if found below then we remove the particular node.
 *
 * Mapper:-- check if Sequence length and coverage are > than the min threshold
 *        -- if true, output node data
 *        -- otherwise, we send messages to the node's neighbors telling them
 *            to delete their edges to the low coverage node
 *
 * Reducer:-- Check if RemoveLowCoverage node field is set
 *         -- if yes; then its the normal node data
 *         -- else, we have the neighbors we want to node(s) to disconnect from
 *         -- remove the neighbors from the node
 */

public class RemoveLowCoverageAvro extends MRStage {
  private static final Logger sLogger =
      Logger.getLogger(RemoveLowCoverageAvro.class);

  public final static CounterName NUM_REMOVED =
      new CounterName("Contrail", "remove-low-coverage-num-removed");

  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(Schema.create(Schema.Type.STRING),
          (new RemoveNeighborMessage()).getSchema());

  public final static CounterName NUM_ISLANDS =
      new CounterName("Contrail", "low-coverage-islands");

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition lengthThresh = new ParameterDefinition("length_thresh",
        "A threshold for sequence lengths. Only sequence's with lengths less " +
        "than this value will be removed if the coverage is low",
        Integer.class, null);
    ParameterDefinition lowCovThresh = new ParameterDefinition("low_cov_thresh",
        "A threshold for node coverage. Only nodes with coverage less " +
            "than this value will be removed ",  Float.class, null);

    ParameterDefinition minLength = new ParameterDefinition(
        "min_length",
        "Nodes with length less than min length will be removed regardless " +
        "of the coverage. ",  Integer.class, 0);

    for (ParameterDefinition def:
             new ParameterDefinition[] {
                lengthThresh, lowCovThresh, minLength}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);

    return Collections.unmodifiableMap(defs);
  }

  private static Pair<CharSequence, RemoveNeighborMessage> out_pair =
      new Pair<CharSequence, RemoveNeighborMessage>(MAP_OUT_SCHEMA);

  public static class RemoveLowCoverageAvroMapper extends
      AvroMapper<GraphNodeData, Pair<CharSequence, RemoveNeighborMessage>>  {
    int lengthThresh;
    float lowCovThresh;
    int minLength;
    GraphNode node = null;
    RemoveNeighborMessage msg = null;

    // List of neighbors to send messages to.
    HashSet<String> neighbors;

    public void configure(JobConf job) {
      RemoveLowCoverageAvro stage = new RemoveLowCoverageAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      minLength = (Integer)(definitions.get("min_length").parseJobConf(job));
      lengthThresh = (Integer)(definitions.get("length_thresh").parseJobConf(job));
      lowCovThresh = (Float)(definitions.get("low_cov_thresh").parseJobConf(job));
      node = new GraphNode();
      msg = new RemoveNeighborMessage();
      neighbors = new HashSet<String>();
    }

    public void map(GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, RemoveNeighborMessage>> output, Reporter reporter) throws IOException  {
      neighbors.clear();
      node.setData(graph_data);
      int len = graph_data.getSequence().getLength();
      float cov = node.getCoverage();

      if (len < minLength) {
        // Node is too short.
        reporter.incrCounter("contrail", "node-too-short", 1);
      } else if ((len > lengthThresh) || (cov >= lowCovThresh)) {
        RemoveNeighborMessage msg = new RemoveNeighborMessage();
        msg.setNode(graph_data);
        msg.setNodeIDtoRemove("");
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);
        return;
      }

      reporter.incrCounter(NUM_REMOVED.group, NUM_REMOVED.tag, 1);
      // We are sending messages to all nodes with edges to this node telling
      // them to remove edges to this terminal.
      int degree = 0;
      for(DNAStrand strand : DNAStrand.values())  {
        degree += node.degree(strand);
        List<EdgeTerminal> terminals = node.getEdgeTerminals(strand, EdgeDirection.INCOMING);
        for(EdgeTerminal terminal : terminals) {
          if (neighbors.contains(terminal.nodeId)) {
            // We've already sent a message to this node.
            continue;
          }
          neighbors.add(terminal.nodeId);
          msg.setNode(null);
          msg.setNodeIDtoRemove(node.getNodeId());
          out_pair.set(terminal.nodeId, msg);
          output.collect(out_pair);
        }
      }
      if (degree == 0)  {
        reporter.incrCounter(NUM_ISLANDS.group, NUM_ISLANDS.tag, 1);
      }
    }
  }

  public static class RemoveLowCoverageAvroReducer extends
      AvroReducer<CharSequence, RemoveNeighborMessage,  GraphNodeData> {
    GraphNode node = null;
    List<String> neighbors = null;

    int lengthThresh;
    float lowCovThresh;

    public void configure(JobConf job) {
      node = new GraphNode();
      RemoveLowCoverageAvro stage = new RemoveLowCoverageAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      neighbors = new ArrayList<String>();
      lengthThresh =
          (Integer)(definitions.get("length_thresh").parseJobConf(job));
      lowCovThresh =
          (Float)(definitions.get("low_cov_thresh").parseJobConf(job));
    }

    public void reduce(CharSequence nodeid, Iterable<RemoveNeighborMessage> iterable,
        AvroCollector< GraphNodeData> output, Reporter reporter) throws IOException {
      Iterator<RemoveNeighborMessage> iter = iterable.iterator();
      neighbors.clear();
      int degree = 0;
      int sawnode = 0;

      while(iter.hasNext())  {
        RemoveNeighborMessage msg= iter.next();
        // normal node
        if (msg.getNode() != null)  {
          node.setData(msg.getNode());
          node = node.clone();
          sawnode++;
        }
        // low coverage nodeID
        else  {
          // Important we need to make a copy of the id before adding it to
          // the list otherwise it will be overwritten on the call to
          // iter.nex(). Calling toString has the effect of making a copy.
          neighbors.add(msg.getNodeIDtoRemove().toString());
        }
      }
      if (sawnode > 1) {
        throw new IOException("ERROR: Saw multiple nodemsg (" + sawnode + ") for " + nodeid.toString());
      }

      if (sawnode == 0) {
        // The node was removed because it had low coverage.
        return;
      }

      for(CharSequence neighbor : neighbors) {
        NeighborData result = node.removeNeighbor(neighbor.toString());
        if(result == null) {
          throw new RuntimeException(
              "ERROR: Edge could not be removed from " + nodeid.toString() +
              " to low coverage node " + neighbor.toString());
        }
        reporter.incrCounter("Contrail", "links-removed", 1);
      }

      degree = node.degree(DNAStrand.FORWARD) + node.degree(DNAStrand.REVERSE);

      // all the neighbors got disconnected
      if (degree == 0)  {
        if ((node.getSequence().size() <= lengthThresh) &&
            (node.getCoverage() < lowCovThresh)) {
          reporter.incrCounter("Contrail", "isolated-nodes-removed", 1);
          reporter.incrCounter(NUM_REMOVED.group, NUM_REMOVED.tag, 1);
          return;
        }
        reporter.incrCounter("Contrail", "isolated-nodes-kept", 1);
      }
      output.collect(node.getData());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, RemoveLowCoverageAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, graph_data.getSchema());

    AvroJob.setMapperClass(conf, RemoveLowCoverageAvroMapper.class);
    AvroJob.setReducerClass(conf, RemoveLowCoverageAvroReducer.class);
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> items = super.validateParameters();

    float coverageThreshold = (Float) stage_options.get("low_cov_thresh");
    int lengthThreshold = (Integer) stage_options.get("length_thresh");

    if (coverageThreshold <= 0) {
      InvalidParameter item = new InvalidParameter(
          "low_cov_thresh",
          "RemoveLowCoverage will not run because "+
          "low_cov_threshold<=0 so no nodes would be removed.");
      items.add(item);
    }

    int K = (Integer)stage_options.get("K");

    if (lengthThreshold <= K) {
      InvalidParameter item = new InvalidParameter(
          "length_thresh",
          "RemoveLowCoverage will not run because "+
          "length_thresh<=K so no nodes would be removed.");
      items.add(item);
    }

    return items;
  }

  @Override
  protected void postRunHook() {
    try {
      long numIslands = job.getCounters().findCounter(
          NUM_ISLANDS.group, NUM_ISLANDS.tag).getValue();
      long numRemoved = job.getCounters().findCounter(
          NUM_REMOVED.group, NUM_REMOVED.tag).getValue();
      long numNodes = job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "REDUCE_OUTPUT_RECORDS").getValue();
      sLogger.info("Number of low coverage nodes removed:" + numRemoved);
      sLogger.info("Number of island nodes removed:" + numIslands);
      sLogger.info("Number of nodes in graph:" + numNodes);
    } catch (IOException e) {
      sLogger.fatal("Couldn't get counters.", e);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RemoveLowCoverageAvro(), args);
    System.exit(res);
  }
}