package contrail.stages;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

/**
 * This mapreduce stage marks nodes which can be compressed. A node
 * can be compressed if it is part of a linear chain; i.e there is a single
 * path through that node.
 */
public class CompressibleAvro extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      CompressibleAvro.class);

  public static final Schema graph_node_data_schema =
      (new GraphNodeData()).getSchema();

  /**
   * Define the schema for the mapper output. The keys will be a string
   * representing the node id. The value will be an instance of
   * CompressibleMapOutput which contains either a node in the graph or
   * a message to a node.
   */
  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(
          Schema.create(Schema.Type.STRING),
          new CompressibleMapOutput().getSchema());

  /**
   * Define the schema for the reducer output. The output is a graph node
   * decorated with information about whether its compressible.
   */
  public static final Schema REDUCE_OUT_SCHEMA =
      (new CompressibleNodeData()).getSchema();

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

  public static class CompressibleMapper extends
    AvroMapper<GraphNodeData, Pair<CharSequence, CompressibleMapOutput>> {

    // Node used to process the graphnode data.
    private GraphNode node = new GraphNode();

    // Output pair to use for the mapper.
    private Pair<CharSequence, CompressibleMapOutput> out_pair = new
        Pair<CharSequence, CompressibleMapOutput>(MAP_OUT_SCHEMA);

    // Message to use; we use a static instance to avoid the cost
    // of recreating one every time we need one.
    private CompressibleMessage message = new CompressibleMessage();

    public void configure(JobConf job) {
      out_pair.value(new CompressibleMapOutput());
    }

    /**
     * Clear the fields in the record.
     * @param record
     */
    protected void clearCompressibleMapOutput(CompressibleMapOutput record) {
      record.setNode(null);
      record.setMessage(null);
    }

    /**
     * Mapper for Compressible.
     *
     * Input is an avro file containing the nodes for the graph.
     * For each input, we output the GraphNodeData keyed by string.
     * We check if this node is part of a linear chain and if it is
     * we send messages to the neighbors letting them know that they
     * are connected to a node which is compressible.
     */
    @Override
    public void map(GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, CompressibleMapOutput>> output,
        Reporter reporter) throws IOException {

      // Set both fields of comp
      node.setData(graph_data);

      CompressibleMapOutput map_output = out_pair.value();

      // We consider the outgoing edges from both strands.
      // Recall that RC(X) -> Y  implies RC(Y) -> X.
      for (DNAStrand strand: DNAStrand.values()) {
        TailData tail = node.getTail(strand, EdgeDirection.OUTGOING);

        if (tail != null) {
          // We have a tail in this direction.
          if (tail.terminal.nodeId.equals(node.getNodeId())) {
            // Cycle so continue.
            continue;
          }

          reporter.incrCounter("Contrail", "remotemark", 1);

          // Send a message to the neighbor telling it this node is compressible.
          clearCompressibleMapOutput(map_output);
          out_pair.key(tail.terminal.nodeId);

          message.setFromNodeId(node.getNodeId());
          StrandsForEdge strands = StrandsUtil.form(
              strand, tail.terminal.strand);
          message.setStrands(strands);
          map_output.setMessage(message);
          output.collect(out_pair);
        }
      }

      // Output the node in the graph.
      out_pair.key(node.getNodeId());
      clearCompressibleMapOutput(map_output);
      map_output.setNode(graph_data);
      out_pair.value(map_output);
      output.collect(out_pair);
      reporter.incrCounter("Contrail", "nodes", 1);
    }
  }

  /**
   * The reducer checks if a node is compressible and if it is marks it
   * with the strands of the node that are compressible before outputting the
   * node.
   */
  public static class CompressibleReducer extends
      AvroReducer<CharSequence, CompressibleMapOutput, CompressibleNodeData> {
    private GraphNode node = new GraphNode();


    // We store the nodes sending messages in two sets, one corresponding
    // to messages to the forward strand and another corresponding to messages
    // the reverse strand.
    private HashSet<EdgeTerminal> f_terminals = new  HashSet<EdgeTerminal>();
    private HashSet<EdgeTerminal> r_terminals = new  HashSet<EdgeTerminal>();

    // The output from the reducer is a node annotated with information
    // about whether its attached to compressible nodes or not.
    private CompressibleNodeData annotated_node =
        new CompressibleNodeData();

    // Clear the data in the node.
    private void clearAnnotatedNode(CompressibleNodeData node) {
      node.setNode(null);
      node.setCompressibleStrands(CompressibleStrands.NONE);
    }

    @Override
    public void reduce(
        CharSequence  node_id, Iterable<CompressibleMapOutput> iterable,
        AvroCollector<CompressibleNodeData> collector, Reporter reporter)
            throws IOException {

      boolean has_node = false;
      f_terminals.clear();
      r_terminals.clear();
      clearAnnotatedNode(annotated_node);

      Iterator<CompressibleMapOutput> iter = iterable.iterator();

      while (iter.hasNext()) {
        // We need to make copies because the iterable returned by hadoop
        // tends to reuse objects.
        CompressibleMapOutput output = iter.next();
        if (output.getMessage() != null && output.getNode() != null) {
          // This is an error only 1 should be set.
          reporter.incrCounter(
              "Contrail", "compressible-error-message-and-node-set", 1);
          continue;
        }
        if (output.getNode() != null ) {
          if (has_node) {
            reporter.incrCounter(
                "Contrail", "compressible-error-node-duplicated", 1);
            throw new RuntimeException(
                "Error: Node " + node_id + " appeared multiple times.");
          }
          node.setData(output.getNode());
          node = node.clone();
          has_node = true;
        }

        if (output.getMessage() != null) {
          CompressibleMessage msg = output.getMessage();
          // Edge is always an outgoing message from the source node.
          // So we reverse the edge strands to get the outgoing edges
          // from this node.
          StrandsForEdge strands = StrandsUtil.complement(msg.getStrands());
          EdgeTerminal terminal = new EdgeTerminal(
              msg.getFromNodeId().toString(),
              StrandsUtil.dest(strands));

          DNAStrand this_strand = StrandsUtil.src(strands);
          if (this_strand == DNAStrand.FORWARD) {
            f_terminals.add(terminal);
          } else {
            r_terminals.add(terminal);
          }
        }
      }

      if (!has_node) {
        throw new RuntimeException(
            "Error: No node provided for node: " + node_id);
      }

      annotated_node.setNode(node.getData());

      /*
       * Now check if this node is compressible.
       * Suppose we have edge X->Y. We can compress this edge
       * if X has a single outgoing edge to Y and Y has a single
       * incoming edge from X. We look at the tail for X to see
       * if it has a single outgoing edge. The messages stored in
       * f_terminals and r_terminals tells us if the node Y has a single
       * incoming edge from X.
       */
      boolean f_compressible = false;
      boolean r_compressible = false;
      for (DNAStrand strand: DNAStrand.values()) {
        // The annotated node always gives the compressible direction
        // with respect to the forward strand.

        // EdgeDirection compressible_direction = EdgeDirection.OUTGOING;
        HashSet<EdgeTerminal> terminals = f_terminals;
        if (strand == DNAStrand.REVERSE) {
          // If we can compress the reverse strand along its outgoing edge
          // that corresponds to compressing the forward strand along its
          // incoming edge.
          //compressible_direction = EdgeDirection.INCOMING;
          terminals = r_terminals;
        }

        TailData tail_data = node.getTail(
            strand, EdgeDirection.OUTGOING);

        if (tail_data == null) {
          // There's no tail in this direction so we can't compress.
          continue;
        }

        // Since there is a tail for this strand, check if there is a message
        // from the node in the tail.
        if (!terminals.contains(tail_data.terminal)) {
          // We can't compress.
          continue;
        }

        // Sanity check since we have a tail in this direction we should
        // have at most a single message.
        if (terminals.size() > 1) {
          // We use an exception for now because we should
          // treat this as an unrecoverable error.
          throw new RuntimeException(
              "Node: " + node.getNodeId() + "has a tail for strand: " +
              strand + " but has more " +
              "than 1 message for this strand. Number of messages is " +
              terminals.size());
        }

        if (strand == DNAStrand.FORWARD) {
          f_compressible = true;
        }
        if (strand == DNAStrand.REVERSE) {
          r_compressible = true;
        }

        reporter.incrCounter(
            GraphCounters.compressible_nodes.group,
            GraphCounters.compressible_nodes.tag, 1);
      }
      annotated_node.setCompressibleStrands(CompressibleStrands.NONE);
      if (f_compressible && r_compressible) {
        annotated_node.setCompressibleStrands(CompressibleStrands.BOTH);
      } else if (f_compressible) {
        annotated_node.setCompressibleStrands(CompressibleStrands.FORWARD);
      } else if (r_compressible) {
        annotated_node.setCompressibleStrands(CompressibleStrands.REVERSE);
      }
      collector.collect(annotated_node);
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), CompressibleAvro.class);
    } else {
      conf = new JobConf(CompressibleAvro.class);
    }
    conf.setJobName("CompressibleAvro " + inputPath);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, CompressibleAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, CompressibleAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, CompressibleMapper.class);
    AvroJob.setReducerClass(conf, CompressibleReducer.class);

    RunningJob result = null;
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
      result = JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);

      System.out.println("Runtime: " + diff + " s");
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CompressibleAvro(), args);
    System.exit(res);
  }
}
