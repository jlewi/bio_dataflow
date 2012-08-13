package contrail.stages;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphError;
import contrail.graph.GraphErrorCodes;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.ValidateEdge;
import contrail.graph.ValidateMessage;

import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

/**
 * This MR job checks that a graph is valid.
 *
 * Every node sends a message to its neighbor containing:
 *   1) The id of the source node.
 *   2) The last K-1 bases of the source node.
 *   3) The strands for the edge.
 *
 * The reducer checks the following
 *   1) The destination node exists.
 *   2) The edge is valid i.e they overlap.
 *   3) The destination node has an incoming edge corresponding to that
 *      edge.
 */
public class ValidateGraph extends Stage {
  private static final Logger sLogger = Logger.getLogger(ValidateGraph.class);

  protected Map<String, ParameterDefinition>
    createParameterDefinitions() {
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
   * For each node, the mapper sends messages to all the neighbors on
   * outgoing edges.
   */
  protected static class ValidateGraphMapper extends
  AvroMapper<GraphNodeData, Pair<CharSequence, ValidateMessage>> {
    private int K = 0;
    private GraphNode node = null;
    private Pair<CharSequence, ValidateMessage> outPair = null;
    public void configure(JobConf job) {
      ValidateGraph stage = new ValidateGraph();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));
      if (K <= 0) {
        throw new RuntimeException("K must be a positive integer");
      }
      node = new GraphNode();
      outPair = new Pair<CharSequence, ValidateMessage> (
          "", new ValidateMessage());
    }
    @Override
    public void map(GraphNodeData nodeData,
        AvroCollector<Pair<CharSequence, ValidateMessage>> output,
        Reporter reporter) throws IOException {

      node.setData(nodeData);
      // Output the node.
      outPair.key(node.getNodeId());
      outPair.value().setNode(nodeData);
      outPair.value().setEdgeInfo(null);
      output.collect(outPair);

      outPair.value().setNode(null);

      ValidateEdge edgeInfo = new ValidateEdge();
      // For each strand get all the outgoing edges.
      for (DNAStrand strand: DNAStrand.values()) {
        List<EdgeTerminal> terminals =
            node.getEdgeTerminals(strand, EdgeDirection.OUTGOING);

        Sequence sequence = node.getSequence();
        sequence = DNAUtil.sequenceToDir(sequence, strand);
        sequence = sequence.subSequence(
            sequence.size() - K +1, sequence.size());

        edgeInfo.setSourceId(node.getNodeId());
        edgeInfo.setOverlap(sequence.toCompressedSequence());
        for (EdgeTerminal terminal: terminals) {
          edgeInfo.setStrands(StrandsUtil.form(strand, terminal.strand));
          outPair.key(terminal.nodeId);
          outPair.value().setEdgeInfo(edgeInfo);
          output.collect(outPair);
        }
      }
    }
  }

  /**
   * Reducer checks the edges and the nodes match up. For each error
   * detected the reducer outputs an instance of GraphError describing the
   * error.
   */
  public static class ValidateGraphReducer extends
      AvroReducer<CharSequence, ValidateMessage, GraphError> {
    private GraphNode node = null;
    private int nodeCount = 0;

    // Keep track of the incoming edges. We group the edges based
    // on the strand of the node they are connected to.
    private HashMap<DNAStrand, List<EdgeInfo>> edges;
    private List<GraphError> edgeErrors;

    // Class to contain the information needed to validate edges.
    static private class EdgeInfo {
      public String sourceId;
      public StrandsForEdge strands;
      public Sequence overlap;
      public EdgeInfo(ValidateEdge info) {
        sourceId = info.getSourceId().toString();
        strands = info.getStrands();
        overlap = new Sequence(DNAAlphabetFactory.create());
        overlap.readPackedBytes(
            info.getOverlap().getDna().array(), info.getOverlap().getLength());
      }
    }

    public void configure(JobConf job) {
      node = new GraphNode();
      edges = new HashMap<DNAStrand, List<EdgeInfo>> ();
      edges.put(DNAStrand.FORWARD, new ArrayList<EdgeInfo>());
      edges.put(DNAStrand.REVERSE, new ArrayList<EdgeInfo>());
      edgeErrors = new ArrayList<GraphError>();
    }

    private void parseInputs(Iterable<ValidateMessage> iterable) {
      Iterator<ValidateMessage> iter = iterable.iterator();
      nodeCount = 0;
      edges.get(DNAStrand.FORWARD).clear();
      edges.get(DNAStrand.REVERSE).clear();

      while(iter.hasNext()) {
        ValidateMessage message  = iter.next();

        if (message.getNode() != null) {
          nodeCount++;
          node.setData(message.getNode());
          node = node.clone();
        }

        if (message.getEdgeInfo() != null) {
          EdgeInfo edgeInfo = new EdgeInfo(message.getEdgeInfo());
          DNAStrand destStrand = StrandsUtil.dest(edgeInfo.strands);
          edges.get(destStrand).add(edgeInfo);
        }
      }
    }

    /**
     * Check if we have a single node for this id.
     */
    private GraphError checkHasNode(CharSequence nodeId) {
      StringBuilder builder = new StringBuilder();
      if (nodeCount == 0) {
        builder.append("Missing node for nodeId: " + nodeId + ".");
        // Get a list of the nodes with edges to the missing node.
        HashSet<String> sourceIds = new HashSet<String>();
        for (DNAStrand strand: DNAStrand.values()) {
          for (EdgeInfo edgeInfo: edges.get(strand)) {
            sourceIds.add(edgeInfo.sourceId);
          }
        }
        builder.append(" Nodes with edges to this node: ");
        for (String sourceId: sourceIds) {
          builder.append(sourceId);
          builder.append(",");
        }
        // Delete the last comma
        builder.deleteCharAt(builder.length() -1 );
        GraphError error = new GraphError();
        error.setErrorCode(GraphErrorCodes.MISSING_NODE);
        error.setMessage(builder.toString());
        return error;
      }

      if (nodeCount > 1) {
        builder.append("Multiple nodes for nodeId: " + nodeId + ".");
        GraphError error = new GraphError();
        error.setErrorCode(GraphErrorCodes.DUPLICATE_NODE);
        error.setMessage(builder.toString());
        return error;
      }
      return null;
    }

    /**
     * Check if the edges are valid. Return false if edges are inavlid.
     * @return
     */
    private boolean checkEdges () {
      boolean isValid = true;
      edgeErrors.clear();

      for (DNAStrand strand: DNAStrand.values()) {
        Sequence sequence = node.getSequence();
        sequence = DNAUtil.sequenceToDir(sequence, strand);

        Sequence overlap = sequence;

        HashSet<EdgeTerminal> incoming = new HashSet<EdgeTerminal>();
        incoming.addAll(node.getEdgeTerminals(
            strand, EdgeDirection.INCOMING));

        for (EdgeInfo edgeInfo: edges.get(strand)) {
          // Check the sequences overlap.
          if (overlap.size() != edgeInfo.overlap.size()) {
            overlap = sequence.subSequence(0, edgeInfo.overlap.size());
          }
          if (overlap.equals(edgeInfo.overlap) == false) {
            isValid = false;
            StringBuilder builder = new StringBuilder();
            builder.append(
                "The overlap for the edge from: " + edgeInfo.sourceId + " ");
            builder.append("to:" + node.getNodeId() + " is not valid.");
            GraphError error = new GraphError();
            error.setErrorCode(GraphErrorCodes.OVERLAP);
            error.setMessage(builder.toString());
            edgeErrors.add(error);
            continue;
          }

          EdgeTerminal terminal = new EdgeTerminal(
              edgeInfo.sourceId, StrandsUtil.src(edgeInfo.strands));
          if (!incoming.contains(terminal)) {
            isValid = false;
            StringBuilder builder = new StringBuilder();
            builder.append(
                "Node:" + edgeInfo.sourceId + " has an edge to:" +
                 node.getNodeId() + " ");
            builder.append("but node:" + node.getNodeId() + " doesn't have ");
            builder.append("an incoming edge from:" + edgeInfo.sourceId);
            GraphError error = new GraphError();
            error.setErrorCode(GraphErrorCodes.MISSING_EDGE);
            error.setMessage(builder.toString());
            edgeErrors.add(error);
          }
        }
      }
      return isValid;
    }

    @Override
    public void reduce(CharSequence nodeId, Iterable<ValidateMessage> iterable,
        AvroCollector<GraphError> collector, Reporter reporter)
            throws IOException {
      parseInputs(iterable);
      GraphError nodeError = checkHasNode(nodeId);
      if (nodeError != null) {
        collector.collect(nodeError);
        reporter.incrCounter("Contrail", "errors-graph", 1);
        return;
      }

      if (!checkEdges()) {
        for (GraphError error: edgeErrors) {
          reporter.incrCounter("Contrail", "errors-graph", 1);
          collector.collect(error);
        }
        return;
      }
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    // Check for missing arguments.
    String[] required_args = {"inputpath", "outputpath", "K"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    int K = (Integer)stage_options.get("K");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("BuildGraph " + inputPath + " " + K);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData nodeData = new GraphNodeData();

    Pair<CharSequence, ValidateMessage> mapPair = new Pair(
        "", new ValidateMessage());

    GraphError graphError = new GraphError();

    AvroJob.setInputSchema(conf, nodeData.getSchema());
    AvroJob.setMapOutputSchema(conf, mapPair.getSchema());
    AvroJob.setOutputSchema(conf, graphError.getSchema());

    AvroJob.setMapperClass(conf, ValidateGraphMapper.class);
    AvroJob.setReducerClass(conf, ValidateGraphReducer.class);

    if (stage_options.containsKey("writeconfig")) {
      writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already
      Path outObj = new Path(outputPath);
      if (FileSystem.get(conf).exists(outObj)) {
        // TODO(jlewi): We should only delete an existing directory
        // if explicitly told to do so.
        sLogger.info("Deleting output path: " + outObj.toString() + " " +
            "because it already exists.");
        FileSystem.get(conf).delete(outObj, true);
      }

      RunningJob result = JobClient.runJob(conf);
      return result;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ValidateGraph(), args);
    System.exit(res);
  }
}
