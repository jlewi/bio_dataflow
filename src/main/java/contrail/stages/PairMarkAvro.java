// Author: Michael Schatz, Jeremy Lewi
package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;
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
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;

/**
 * The first map-reduce stage for a probabilistic algorithm for merging linear
 * chains.
 *
 * Suppose we have a linear chain of nodes A->B->C->D...-E. All of these
 * nodes can be merged into one node through a series of local merges,
 * e.g A + B = AB, C+D= DC, .. AB+DC = ABDC, etc... When performing these
 * merges in parallel, we need to make sure that a node doesn't get merged
 * twice. For example, if B is merged into A, we shouldn't simultaneously
 * merge B into C. Another challenge, is that when we merge two nodes we
 * need to send messages to those nodes with edges to these nodes, letting
 * them know the new strand and node id that corresponds to the node
 * which has been merged away.
 *
 * In this stage, we identify which nodes will be sent to other nodes to be
 * merged. Furthermore, edges are updated so they point to what will be the
 * new merged node. The actual merge, however, doesn't happen until the next
 * stage.
 *
 * Some key aspects for the merge are:
 * 1. Each compressible node is randomly assigned a state of Up or Down.
 * 2. The Up/Down state is determined from a global seed and the nodeid.
 *    Thus any node can compute the state for any other node.
 * 3. Down nodes preserve i) their id and ii) their strand.
 *    Thus, if a down node stores the sequence A and is merged with its
 *    neighbor storing B, the merged node will always store the sequence
 *    AB as the forward strand of the merged node. This means, the sequence
 *    stored in nodes after the merge may NOT BE the Canonical sequence.
 *
 *    This is necessary, to allow edge updates to be properly propogated in all
 *    cases.
 *
 * The mapper does the following.
 * 1. Randomly assign up and down states to nodes.
 * 2. Identify special cases in which a down state may be converted to an up
 *    state. (This is an optimization which increases the number of merges
 *    performed).
 * 3. Form edge update messages.
 *    Suppose we have  A->B->C
 *    If B is merged into A. Then the mapper constructs a message to inform
 *    C of the new id and strand for node B so that A can move its edges
 *    to B to C.
 * 4. Output each node, keyed by its id in the graph. If the node is an up
 *    node to be merged, then the mapper identifies it as such and marks
 *    the strand which will be merged.
 *
 * The reducer does the following:
 * 1. Apply the edge updates to the node.
 * 2. Output each node, along with information about which node it will be
 *    merged with if applicable.
 *
 * The conditions for deciding when to merge an up node into a down node are
 * as follows.
 * 1. If a down node is in between two down nodes, and the node has the
 *    smallest id of its neighbors convert the node to an up node and merge it.
 * 2. If a down node is compressible along a single strand, and its compressible
 *    neighbor is a down node, and the node has the smaller node id. Convert
 *    it to an up node.
 * 3. If node is an up node merge it with one of its neighboring down nodes.
 */
public class PairMarkAvro extends Stage {
  private static final Logger sLogger = Logger.getLogger(PairMarkAvro.class);

  /**
   * A wrapper class for the CompressibleNodeData schema.
   */
  private static class CompressibleNode {
    private CompressibleNodeData data;
    private GraphNode node;

    public CompressibleNode() {
      // data gets set with setData.
      data = null;
      node = new GraphNode();
    }
    public void setData(CompressibleNodeData new_data) {
      data = new_data;
      node.setData(new_data.getNode());
    }

    public GraphNode getNode() {
      return node;
    }

    /**
     * Whether the given strand of the node can be compressed.
     * @param strand
     * @return
     */
    public boolean canCompress(DNAStrand strand){
      if (data.getCompressibleStrands() == CompressibleStrands.BOTH) {
        return true;
      }
      if (strand == DNAStrand.FORWARD &&
          data.getCompressibleStrands() == CompressibleStrands.FORWARD) {
        return true;
      }
      if (strand == DNAStrand.REVERSE &&
          data.getCompressibleStrands() == CompressibleStrands.REVERSE) {
        return true;
      }
      return false;
    }

    public String toString() {
      if (node == null) {
        return "";
      }
      return node.toString();
    }
  }

  protected static class PairMarkMapper extends
  AvroMapper<CompressibleNodeData, Pair<CharSequence, PairMarkOutput>> {
    private CompressibleNode compressible_node;
    private CoinFlipper flipper;
    private NodeInfoForMerge node_info_for_merge;
    // The output for the mapper.
    private Pair<CharSequence, PairMarkOutput> out_pair;

    public void configure(JobConf job) {
      compressible_node = new CompressibleNode();

      PairMarkAvro stage = new PairMarkAvro();
      Map<String, ParameterDefinition> parameters
        = stage.getParameterDefinitions();
      long randseed = (Long) parameters.get("randseed").parseJobConf(job);
      flipper = new CoinFlipper(randseed);
      out_pair = new Pair<CharSequence, PairMarkOutput>(
          "", new PairMarkOutput());
      node_info_for_merge = new NodeInfoForMerge();
    }

    public TailData getBuddy(CompressibleNode node, DNAStrand strand) {
      if (node.canCompress(strand)) {
        return node.getNode().getTail(strand, EdgeDirection.OUTGOING);
      }
      return null;
    }

    // Container class for storing information about which edge in this node
    // to compress.
    private static class EdgeToCompress {
      public EdgeToCompress(EdgeTerminal terminal, DNAStrand dna_strand) {
        other_terminal = terminal;
        strand = dna_strand;
      }
      // This is the terminal for the edge we are going to compress.
      public EdgeTerminal other_terminal;

      // This is the strand of this node which is connected to
      // other_terminal and which gets compressed.
      public DNAStrand strand;
    }

    /**
     * Handle a node which is a assigned Up.
     * Since the node is assigned Up we can merge it with a neighbor if
     * the neighbor is assigned Down.
     * @param node: Node to process.
     * @param fbuddy: Tail information for the forward strand.
     * @param rbuddy: Tail information for the reverse strand.
     * @return: Information about the strand to compress.
     */
    private EdgeToCompress processUpNode(
        CompressibleNode node, TailData fbuddy, TailData rbuddy) {
      // Prefer Merging forward if we can.
      // We can only merge in a single direction at a time.
      if (fbuddy != null) {
        CoinFlipper.CoinFlip f_flip = flipper.flip(fbuddy.terminal.nodeId);

        if (f_flip == CoinFlipper.CoinFlip.DOWN) {
          // We can compress the forward strand.
          return new EdgeToCompress(fbuddy.terminal, DNAStrand.FORWARD);
        }
      }

      // If we can't compress the forward strand, see if
      // we can compress the reverse strand.
      if (rbuddy != null) {
        CoinFlipper.CoinFlip r_flip = flipper.flip(rbuddy.terminal.nodeId);

        if (r_flip == CoinFlipper.CoinFlip.DOWN) {
          return new EdgeToCompress(rbuddy.terminal, DNAStrand.REVERSE);
        }
      }

      // Can't do a merge.
      return null;
    }

    // Send update messages to the neighbors.
    private void updateEdges(
        EdgeToCompress edge_to_compress,
        AvroCollector<Pair<CharSequence, PairMarkOutput>> collector)
            throws IOException {
      EdgeUpdateForMerge edge_update = new EdgeUpdateForMerge();
      edge_update.setOldId(compressible_node.getNode().getNodeId());
      edge_update.setOldStrand(edge_to_compress.strand);
      edge_update.setNewId(edge_to_compress.other_terminal.nodeId);
      edge_update.setNewStrand(edge_to_compress.other_terminal.strand);

      List<EdgeTerminal> incoming_terminals =
          compressible_node.getNode().getEdgeTerminals(
              edge_to_compress.strand, EdgeDirection.INCOMING);

      for (EdgeTerminal terminal: incoming_terminals){
        out_pair.key(terminal.nodeId);
        out_pair.value().setPayload(edge_update);
        collector.collect(out_pair);
      }
    }

    // If this node and its neighbors were all assigned a state of Down then we
    // can potentially convert this node to Up and merge it with one of its
    // neighbors.
    // To ensure two adjacent nodes aren't both forced to up, we only convert
    // the node if it has the smallest node id among its two neighbors.
    private boolean convertDownToUp(
        GraphNode node, TailData fbuddy, TailData rbuddy) {
      // If a down node is between two down nodes and it has the smallest
      // id then we can convert it to up.
      if ((rbuddy != null) && (fbuddy != null)) {
        // We have tails for both strands of this node.
        CoinFlipper.CoinFlip f_flip = flipper.flip(fbuddy.terminal.nodeId);
        CoinFlipper.CoinFlip r_flip = flipper.flip(rbuddy.terminal.nodeId);

        if (f_flip == CoinFlipper.CoinFlip.DOWN &&
            r_flip == CoinFlipper.CoinFlip.DOWN &&
            (node.getNodeId().compareTo(
                fbuddy.terminal.nodeId) < 0) &&
                (node.getNodeId().compareTo(
                    rbuddy.terminal.nodeId) < 0)) {
          // Both neighbors are down nodes and this node has the smallest
          // id among the trio, therefore we force this node to be up.
          return true;
        }
        return false;
      }

      // If the node is compressible along a single strand and its neighbor
      // is a down nodes, and the node has the smallest id then we can
      // convert it to up.
      String neighbor = null;

      if (fbuddy != null) {
        neighbor = fbuddy.terminal.nodeId;
      } else if (rbuddy != null) {
        neighbor = rbuddy.terminal.nodeId;
      } else {
        // Its not compressible. This code should never be reached
        // because convertDownToUp should only be invoked if its compressible
        // along at least one strand.
        return false;
      }

      CoinFlipper.CoinFlip flip = flipper.flip(neighbor);
      if (flip == CoinFlipper.CoinFlip.DOWN && (
          node.getNodeId().compareTo(neighbor) < 0)) {
        return true;
      }
      return false;
    }

    /**
     * Compute the state for this node.
     * @param fbuddy
     * @param rbuddy
     * @return
     */
    private CoinFlipper.CoinFlip computeState(
        GraphNode node, TailData fbuddy, TailData rbuddy) {
      CoinFlipper.CoinFlip coin = flipper.flip(node.getNodeId());
      // If this node is randomly assigned Down, see if it can be converted
      // to up.
      if (coin == CoinFlipper.CoinFlip.UP) {
        return coin;
      }

      if (convertDownToUp(node, fbuddy, rbuddy)) {
        return CoinFlipper.CoinFlip.UP;
      }
      return coin;
    }

    public void map(CompressibleNodeData node_data,
        AvroCollector<Pair<CharSequence, PairMarkOutput>> collector,
        Reporter reporter) throws IOException {
      compressible_node.setData(node_data);
      // Check if either the forward or reverse strand can be merged.
      TailData fbuddy = getBuddy(compressible_node, DNAStrand.FORWARD);
      TailData rbuddy = getBuddy(compressible_node, DNAStrand.REVERSE);

      if (fbuddy == null && rbuddy == null) {
        // Node can't be compressed so output the node and we are done.
        out_pair.key(node_data.getNode().getNodeId());
        node_info_for_merge.setCompressibleNode(node_data);
        node_info_for_merge.setStrandToMerge(CompressibleStrands.NONE);
        out_pair.value().setPayload(node_info_for_merge);
        collector.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
        return;
      }
      reporter.incrCounter("Contrail", "compressible", 1);

      CoinFlipper.CoinFlip coin = computeState(
          compressible_node.getNode(), fbuddy, rbuddy);

      if (coin == CoinFlipper.CoinFlip.DOWN) {
        // Just output this node since this is a Down node.
        out_pair.key(compressible_node.getNode().getNodeId());
        node_info_for_merge.setCompressibleNode(node_data);
        node_info_for_merge.setStrandToMerge(CompressibleStrands.NONE);
        out_pair.value().setPayload(node_info_for_merge);
        collector.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
        return;
      }

      // Check if this node can be sent to one of its neighbors to be merged.
      EdgeToCompress edge_to_compress =
          processUpNode(compressible_node, fbuddy, rbuddy);

      if (edge_to_compress == null) {
        // This node doesn't get sent to another node be merged.
        out_pair.key(node_data.getNode().getNodeId());
        node_info_for_merge.setCompressibleNode(node_data);
        node_info_for_merge.setStrandToMerge(CompressibleStrands.NONE);
        out_pair.value().setPayload(node_info_for_merge);
        collector.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
        return;
      }

      // Send the messages to update other nodes.
      updateEdges(edge_to_compress, collector);

      // Output this node.
      out_pair.key(node_data.getNode().getNodeId());
      node_info_for_merge.setCompressibleNode(node_data);
      node_info_for_merge.setStrandToMerge(
          CompressUtil.dnaStrandToCompressibleStrands(edge_to_compress.strand));
      out_pair.value().setPayload(node_info_for_merge);
      collector.collect(out_pair);
      reporter.incrCounter(
          GraphCounters.num_nodes_to_merge.group,
          GraphCounters.num_nodes_to_merge.tag, 1);
    }

    /**
     * Sets the coin flipper. This is primarily intended for use by the
     * unittest.
     */
    public void setFlipper(CoinFlipper flipper) {
      this.flipper = flipper;
    }
  }

  protected static class PairMarkReducer extends
    AvroReducer <CharSequence, PairMarkOutput, NodeInfoForMerge> {
    // The output for the reducer.
    private NodeInfoForMerge output_node;

    public void configure(JobConf job) {
      output_node = new NodeInfoForMerge();
    }

    public void reduce(
        CharSequence nodeid, Iterable<PairMarkOutput> iterable,
        AvroCollector<NodeInfoForMerge> collector, Reporter reporter)
            throws IOException {
      Iterator<PairMarkOutput> iter = iterable.iterator();

      boolean seen_node = false;
      GraphNode graph_node = null;
      ArrayList<EdgeUpdateForMerge> edge_updates =
          new ArrayList<EdgeUpdateForMerge>();

      while(iter.hasNext()) {
        PairMarkOutput mark_output = iter.next();
        if (mark_output.getPayload() instanceof NodeInfoForMerge) {
          // Sanity check there should be a single instance of NodeInfoForMerge.
          if (seen_node) {
            throw new RuntimeException(
                "There are two nodes for nodeid: " + nodeid);
          }
          NodeInfoForMerge source =
              (NodeInfoForMerge) mark_output.getPayload();

          // We need to make a copy of the node because iterable
          // will reuse the same instance when next is called.
          // Because of https://issues.apache.org/jira/browse/AVRO-1045 we
          // can't use the Avro methods for copying the data.
          graph_node =
              new GraphNode(source.getCompressibleNode().getNode()).clone();
          source.getCompressibleNode().setNode(null);
          output_node = (NodeInfoForMerge) SpecificData.get().deepCopy(
              source.getSchema(), source);
          output_node.getCompressibleNode().setNode(graph_node.getData());
          seen_node = true;
        } else {
          EdgeUpdateForMerge edge_update =
              (EdgeUpdateForMerge) mark_output.getPayload();
          edge_update =
              (EdgeUpdateForMerge) SpecificData.get().deepCopy(
                  edge_update.getSchema(), edge_update);
          edge_updates.add(edge_update);
        }
      }

      if (!seen_node) {
        throw new RuntimeException(
            "There is no node to output for nodeid: " + nodeid);
      }

      for (EdgeUpdateForMerge edge_update: edge_updates) {
        EdgeTerminal old_terminal = new EdgeTerminal(
            edge_update.getOldId().toString(), edge_update.getOldStrand());

        DNAStrand strand = graph_node.findStrandWithEdgeToTerminal(
            old_terminal, EdgeDirection.OUTGOING);

        if (strand == null) {
          throw new RuntimeException(
              "Node: " + nodeid + " has recieved a message to update edge " +
              "to terminal:" + old_terminal + " but no edge could be found " +
              "to that terminal.");
        }

        EdgeTerminal new_terminal = new EdgeTerminal(
            edge_update.getNewId().toString(), edge_update.getNewStrand());

        graph_node.moveOutgoingEdge(strand, old_terminal, new_terminal);
      }

      collector.collect(output_node);
    }
  }

  /**
   * Return a list of parameters used by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
      new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    ContrailParameters.addList(
        defs, ContrailParameters.getInputOutputPathOptions());

    ParameterDefinition seed
      = new ParameterDefinition(
          "randseed",
          "Seed for the random number generator. Needs to be unique for " +
          "each iteration", Long.class, null);

    defs.put(seed.getName(), seed);
    return Collections.unmodifiableMap(defs);
  }

  public int run(String[] args) throws Exception {
    sLogger.info("Tool name: PairMarkAvro");
    parseCommandLine(args);
    runJob();
    return 0;
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath", "randseed"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    long randseed = (Long) stage_options.get("randseed");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);
    sLogger.info(" - randseed: " + randseed);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), PairMarkAvro.class);
    } else {
      conf = new JobConf(PairMarkAvro.class);
    }

    conf.setJobName("PairMarkAvro " + inputPath);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    Pair<CharSequence, PairMarkOutput> map_output =
        new Pair<CharSequence, PairMarkOutput>
          ("", new PairMarkOutput());
    NodeInfoForMerge reducer_output = new NodeInfoForMerge();
    AvroJob.setInputSchema(conf, compressible_node.getSchema());
    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
    AvroJob.setOutputSchema(conf, reducer_output.getSchema());

    AvroJob.setMapperClass(conf, PairMarkMapper.class);
    AvroJob.setReducerClass(conf, PairMarkReducer.class);

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
    int res = ToolRunner.run(new Configuration(), new PairMarkAvro(), args);
    System.exit(res);
  }
}
