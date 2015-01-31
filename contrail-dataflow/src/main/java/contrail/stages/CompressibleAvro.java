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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.NodeMerger;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.stages.GraphCounters.CounterName;

/**
 * This mapreduce stage marks nodes which can be compressed. A node
 * can be compressed if it is part of a linear chain; i.e there is a single
 * path through that node.
 *
 * The mapper also handles merging nodes with connected strands. For example,
 * the node X->R(X) would be merged to form the sequence XR(X)[-1].
 */
public class CompressibleAvro extends MRStage {
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

  public static CounterName NUM_COMPRESSIBLE =
      new CounterName("Contrail", "compressible");

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

    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);
    return Collections.unmodifiableMap(defs);
  }

  public static class CompressibleMapper extends
    AvroMapper<GraphNodeData, Pair<CharSequence, CompressibleMapOutput>> {

    // Node used to process the graphnode data.
    private GraphNode node = new GraphNode();

    // Output pair to use for the mapper.
    private final Pair<CharSequence, CompressibleMapOutput> out_pair = new
        Pair<CharSequence, CompressibleMapOutput>(MAP_OUT_SCHEMA);

    // Message to use; we use a static instance to avoid the cost
    // of recreating one every time we need one.
    private final CompressibleMessage message = new CompressibleMessage();

    private NodeMerger nodeMerger;

    private int K;
    @Override
    public void configure(JobConf job) {
      nodeMerger = new NodeMerger();
      out_pair.value(new CompressibleMapOutput());

      K = (Integer) ContrailParameters.getK().parseJobConf(job);
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
      node.setData(graph_data);
      CompressibleMapOutput map_output = out_pair.value();

      if (node.hasConnectedStrands()) {
        reporter.incrCounter(
            "Contrail","nodes-with-connected-strands", 1);

        // We merge the strands if possible.
        GraphNode newNode = nodeMerger.mergeConnectedStrands(node, K - 1);
        if (newNode != null) {
          reporter.incrCounter(
              "Contrail","nodes-with-connected-strands-merged", 1);
          node = newNode;
        }
      }

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

          // Send a message to the neighbor telling it this node is
          // compressible.
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
      map_output.setNode(node.getData());
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

    // We store the nodes sending messages in two sets.
    // We use one set store messages to the forward strand and another
    // corresponding to messages to the reverse strand.
    private final HashSet<EdgeTerminal> f_terminals =
        new HashSet<EdgeTerminal>();
    private final HashSet<EdgeTerminal> r_terminals =
        new HashSet<EdgeTerminal>();

    // The output from the reducer is a node annotated with information
    // about whether its attached to compressible nodes or not.
    private final CompressibleNodeData annotated_node =
        new CompressibleNodeData();

    // Clear the data in the node.
    private void clearAnnotatedNode(CompressibleNodeData node) {
      node.setNode(null);
      node.setCompressibleStrands(CompressibleStrands.NONE);
    }

    /**
     * Determine if either of the node's strands are compressible.
     *
     * @return
     */
    private CompressibleStrands isCompressible() {
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
        HashSet<EdgeTerminal> terminals = f_terminals;
        if (strand == DNAStrand.REVERSE) {
          // If we can compress the reverse strand along its outgoing edge
          // that corresponds to compressing the forward strand along its
          // incoming edge.
          terminals = r_terminals;
        }
        if (terminals.size() == 0) {
          // There are no message in this direction so we don't be able
          // to compress it.
          continue;
        }

        TailData tail_data = node.getTail(
            strand, EdgeDirection.OUTGOING);

        if (tail_data == null) {
          // There's no tail in this direction so we can't compress.
          continue;
        }

        // Since there is a tail for this strand, check if there is a message
        // from the node in the tail.
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
        if (!terminals.iterator().next().nodeId.equals(
            tail_data.terminal.nodeId)) {
          EdgeTerminal next = terminals.iterator().next();
          sLogger.fatal(
              "Node: " + node.getNodeId() + "has a tail for strand: " +
              strand + " to terminal " + tail_data.terminal.toString() + " " +
              "but the compressible message is from terminal " +
              next.toString(),
              new RuntimeException("Inconsistent compressible messages."));
        }

        if (strand == DNAStrand.FORWARD) {
          f_compressible = true;
        }
        if (strand == DNAStrand.REVERSE) {
          r_compressible = true;
        }
      }

      if (f_compressible && r_compressible) {
        return CompressibleStrands.BOTH;
      } else if (f_compressible) {
        return CompressibleStrands.FORWARD;
      } else if (r_compressible) {
        return CompressibleStrands.REVERSE;
      }
      return CompressibleStrands.NONE;
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

      CompressibleStrands compressible_strands = null;
      compressible_strands = isCompressible();

      annotated_node.setCompressibleStrands(compressible_strands);
      if (compressible_strands != CompressibleStrands.NONE) {
        reporter.incrCounter(
            NUM_COMPRESSIBLE.group, NUM_COMPRESSIBLE.tag, 1);
      }
      collector.collect(annotated_node);
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
    AvroJob.setMapOutputSchema(conf, CompressibleAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, CompressibleAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, CompressibleMapper.class);
    AvroJob.setReducerClass(conf, CompressibleReducer.class);
  }

  @Override
  protected void postRunHook() {
    try {
      long numNodes = job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "MAP_INPUT_RECORDS").getValue();
      long numCompressibleNodes = job.getCounters().findCounter(
          NUM_COMPRESSIBLE.group, NUM_COMPRESSIBLE.tag).getValue();

      sLogger.info("Number of nodes in graph:" + numNodes);
      sLogger.info(
          "Number of compressible nodes in graph:" + numCompressibleNodes);
    } catch (IOException e) {
      sLogger.fatal("Couldn't get counters.", e);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CompressibleAvro(), args);
    System.exit(res);
  }
}
