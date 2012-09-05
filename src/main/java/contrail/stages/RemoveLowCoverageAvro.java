package contrail.stages;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
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

public class RemoveLowCoverageAvro extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(RemoveLowCoverageAvro.class);

  public final static CounterName NUM_REMOVED =
      new CounterName("Contrail", "remove-low-coverage-num-removed");

  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(Schema.create(Schema.Type.STRING),
          (new RemoveNeighborMessage()).getSchema());

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition lengthThresh = new ParameterDefinition("length_thresh",
        "A threshold for sequence lengths. Only sequence's with lengths less " +
        "than this value will be removed if the coverage is low",
        Integer.class, new Integer(0));
    ParameterDefinition lowCovThresh = new ParameterDefinition("low_cov_thresh",
        "A threshold for node coverage. Only nodes with coverage less " +
            "than this value will be removed ",  Float.class, new Float(0));

    for (ParameterDefinition def:
             new ParameterDefinition[] {lengthThresh, lowCovThresh}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  private static Pair<CharSequence, RemoveNeighborMessage> out_pair =
      new Pair<CharSequence, RemoveNeighborMessage>(MAP_OUT_SCHEMA);

  public static class RemoveLowCoverageAvroMapper extends
  AvroMapper<GraphNodeData, Pair<CharSequence, RemoveNeighborMessage>>  {
    int lengthThresh;
    float lowCovThresh;
    GraphNode node = null;
    RemoveNeighborMessage msg = null;

    public void configure(JobConf job) {
      RemoveLowCoverageAvro stage = new RemoveLowCoverageAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      lengthThresh = (Integer)(definitions.get("length_thresh").parseJobConf(job));
      lowCovThresh = (Float)(definitions.get("low_cov_thresh").parseJobConf(job));
      node = new GraphNode();
      msg = new RemoveNeighborMessage();
    }

    public void map(GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, RemoveNeighborMessage>> output, Reporter reporter) throws IOException  {
      node.setData(graph_data);
      int len = graph_data.getSequence().getLength();
      float cov = node.getCoverage();

      // normal node
      if ((len > lengthThresh) || (cov >= lowCovThresh)) {
        RemoveNeighborMessage msg = new RemoveNeighborMessage();
        msg.setNode(graph_data);
        msg.setNodeIDtoRemove("");
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);
        return;
      }

      reporter.incrCounter(NUM_REMOVED.group, NUM_REMOVED.tag, 1);
      // We are sending messages to all nodes with edges to this node telling them that this node has low coverage
      int degree = 0;
      for(DNAStrand strand : DNAStrand.values())  {
        degree += node.degree(strand);
        List<EdgeTerminal> terminals = node.getEdgeTerminals(strand, EdgeDirection.INCOMING);
        for(EdgeTerminal terminal : terminals) {
          msg.setNode(null);
          msg.setNodeIDtoRemove(node.getNodeId());
          out_pair.set(terminal.nodeId, msg);
          output.collect(out_pair);
        }
      }
      if (degree == 0)  {
        reporter.incrCounter("Contrail", "lowcoverage-island", 1);
      }
    }
  }

  public static class RemoveLowCoverageAvroReducer extends
  AvroReducer<CharSequence, RemoveNeighborMessage,  GraphNodeData> {
    GraphNode node = null;
    List<String> neighbors = null;

    public void configure(JobConf job) {
      node = new GraphNode();
      neighbors = new ArrayList<String>();
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
        NeighborData result= node.removeNeighbor(neighbor.toString());
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
        reporter.incrCounter("Contrail", "isolated-nodes-removed", 1);
        reporter.incrCounter(NUM_REMOVED.group, NUM_REMOVED.tag, 1);
        return;
      }
      output.collect(node.getData());
    }
  }

  @Override
  public RunningJob runJob() throws Exception  {
    String[] required_args =
      {"inputpath", "outputpath", "low_cov_thresh", "length_thresh", "K"};

    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    int K = (Integer)stage_options.get("K");

    sLogger.info("Tool name: RemoveLowCoverage");
    sLogger.info(" - input: " + inputPath);
    sLogger.info(" - output: " + outputPath);

    float coverageThreshold = (Float) stage_options.get("low_cov_thresh");
    int lengthThreshold = (Integer) stage_options.get("length_thresh");

    if (coverageThreshold <= 0) {
      sLogger.warn(
          "RemoveLowCoverage will not run because "+
          "low_cov_threshold<=0 so no nodes would be removed.");
      return null;
    }

    if (lengthThreshold <= K) {
      sLogger.warn(
          "RemoveLowCoverage will not run because "+
          "length_thresh<=K so no nodes would be removed.");
      return null;
    }

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    }
    else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("RemoveLowCoverage " + inputPath);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, RemoveLowCoverageAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, graph_data.getSchema());

    AvroJob.setMapperClass(conf, RemoveLowCoverageAvroMapper.class);
    AvroJob.setReducerClass(conf, RemoveLowCoverageAvroReducer.class);

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
      RunningJob job = JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);

      System.out.println("Runtime: " + diff + " s");
      return job;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RemoveLowCoverageAvro(), args);
    System.exit(res);
  }
}