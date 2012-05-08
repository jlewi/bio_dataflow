// Author: Michael Schatz, Jeremy Lewi
package contrail.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.NodeMerger;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;


/**
 *
 */
public class PairMergeAvro extends Stage {
  private static final Logger sLogger = Logger.getLogger(PairMergeAvro.class);

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
  }

  /**
   * Convert the enumeration CompressibleStrands to the equivalent DNAStrand
   * enumeration if possible.
   * @param strands
   * @return
   */
  protected static DNAStrand compressibleStrandsToDNAStrand(
      CompressibleStrands strands) {
    switch (strands) {
      case BOTH:
        return null;
      case NONE:
        return null;
      case FORWARD:
        return DNAStrand.FORWARD;
      case REVERSE:
        return DNAStrand.REVERSE;
      default:
        return null;
    }
  }

  protected static class PairMergeMapper extends
  AvroMapper<CompressibleNodeData, Pair<CharSequence, CompressibleNodeData>> {
    private CompressibleNode node;
    private CoinFlipper flipper;
    public void configure(JobConf job) {
      node = new CompressibleNode();
      flipper = new CoinFlipper(Long.parseLong(job.get("randseed")));
      out_pair = new Pair<CharSequence, CompressibleNodeData>(
          "", new CompressibleNodeData());
    }


    public void map(CompressibleNodeData node_data,
        AvroCollector<Pair<CharSequence, CompressibleNodeData>> collector,
        Reporter reporter) throws IOException {
      node.setData(node_data);

    }

    /**
     * Sets the coin flipper. This is primarily intended for use by the
     * unittest.
     */
    public void setFlipper(CoinFlipper flipper) {
      this.flipper = flipper;
    }
  }

  protected static class PairMergeReducer extends
    AvroReducer <CharSequence, CompressibleNodeData, PairMergeOutput> {

    // The output for the reducer.
    private PairMergeOutput output;

    // The length of the KMers.
    private int K;

    public void configure(JobConf job) {
      K = Integer.parseInt(job.get("K"));
      output = new PairMergeOutput();
      output.setUpdateMessages(new ArrayList<EdgeUpdateAfterMerge>());
      output.setCompressibleNode(null);
    }

    /**
     * Find the strand that connects strand this_strand of node
     * to node other_node.
     *
     * @returns The strand on the other node or null if no edge exists.
     */
    public DNAStrand findTargetStrand(
        GraphNode node, DNAStrand this_strand, String other_node) {
      for (EdgeTerminal terminal:
           node.getEdgeTerminals(this_strand, EdgeDirection.OUTGOING)) {
        if (terminal.nodeId.equals(other_node)) {
          return terminal.strand;
        }
      }
      return null;
    }

    /**
     * This function returns a list of the messages to update edges for the
     * nodes which have been merged.
     * @param node: The node that has been merged. This is the node
     *   we get a list of edges that need to be updated.
     * @param strand: The strand of node that has been merged.
     * @param new_nodeid: The id for the new node that represents node.
     * @param new_strand: The strand of the merged node corresponding to
     *   strand of node.
     * @return: A list of the update messages.
     */
    protected List<EdgeUpdateAfterMerge> updateMessagesForEdge(
        GraphNode node, DNAStrand strand, String new_nodeid,
        DNAStrand new_strand) {
      List<EdgeUpdateAfterMerge> edge_updates =
          new ArrayList<EdgeUpdateAfterMerge> ();
      // For the source node, we need to update the incoming edges
      // to the strand that was merged.
      List<EdgeTerminal> incoming_terminals =
          node.getEdgeTerminals(strand, EdgeDirection.INCOMING);

      for (EdgeTerminal terminal: incoming_terminals) {
        EdgeUpdateAfterMerge update = new EdgeUpdateAfterMerge();
        update.setOldStrands(StrandsUtil.form(terminal.strand, strand));
        update.setNewStrands(StrandsUtil.form(terminal.strand, new_strand));

        update.setOldTerminalId(node.getNodeId());
        update.setNewTerminalId(new_nodeid);

        update.setNodeToUpdate(terminal.nodeId);

        edge_updates.add(update);
      }
      return edge_updates;
    }

    /**
     * Determines whether the merged node resulting from chain is further
     * compressible.
     * @param chain: The chain of nodes which are merged together.
     * @param merged_strand: Which strand corresponds to merging chain
     *   together.
     * @return: Which strands if any of the merged node are compressible.
     */
    protected CompressibleStrands isCompressible(
        ArrayList<ChainLink> chain, DNAStrand merged_strand) {
      // Now we need to determine whether the merged node is compressible.
      // The merged node is compressible if the ends of the chain are
      // compressible in both directions.
      ArrayList<DNAStrand> compressible_strands = new ArrayList<DNAStrand>();

      if (chain.get(0).node.getCompressibleStrands() ==
          CompressibleStrands.BOTH) {
        // Get the strand of node 0 that wasn't compressed.
        DNAStrand strand =
            DNAStrandUtil.flip(chain.get(0).compressible_strand);
        // We need to flip the strand if merged_strand is different
        // from the strand for node 0.
        if (chain.get(0).compressible_strand != merged_strand) {
          strand = DNAStrandUtil.flip(strand);
        }
        compressible_strands.add(strand);
      }

      int tail = chain.size() - 1;
      if (chain.get(tail).node.getCompressibleStrands() ==
          CompressibleStrands.BOTH) {
        // Get the strand of the last node that wasn't compressed.
        // The last node would have been compressed along the incoming
        // edge, so we can still compress it along the outgoing
        // edge.
        DNAStrand strand = chain.get(tail).compressible_strand;
        // We need to flip the strand if merged_strand is different
        // from the strand for the last node.
        if (chain.get(tail).compressible_strand != merged_strand) {
          strand = DNAStrandUtil.flip(strand);
        }
        compressible_strands.add(strand);
      }

      switch (compressible_strands.size()) {
        case 0:
          return CompressibleStrands.NONE;
        case 1:
          if (compressible_strands.get(0) == DNAStrand.FORWARD) {
            return CompressibleStrands.FORWARD;
          } else {
            return CompressibleStrands.REVERSE;
          }
        case 2:
          // Sanity check. The two strands should not be equal.
          if (compressible_strands.get(0) == compressible_strands.get(1)) {
            throw new RuntimeException(
                "There is a bug in the code. The two strands should not be " +
                "the same.");
          }
          return CompressibleStrands.BOTH;
        default:
          throw new RuntimeException("This code should not be reached.");
      }
    }

    /**
     * Merge the nodes together. The result of the merge is stored in the
     * member variable output.
     *
     * @param chain: An array of nodes to merge together. The nodes
     *   should be ordered corresponding to their position in the chain.
     * @param new_id: The new id to assign to the merged nodes.
     */
    protected void mergeChain(
        ArrayList<ChainLink> chain, String new_id) {
      // Check the chain and find out which strand of each node belongs in
      // the chain.
      GraphNode node = new GraphNode();
      for (int pos = 0; pos < chain.size() -1; pos++) {
        node.setData(chain.get(pos).node.getNode());
        TailData tail =
            node.getTail(
                chain.get(pos).compressible_strand, EdgeDirection.OUTGOING);
        if (tail == null) {
          throw new RuntimeException(
              "Nodes don't form a chain. This shouldn't happen and could be " +
              "a bug in the code.");
        }
        if (!tail.terminal.nodeId.equals(
            chain.get(pos + 1).node.getNode().getNodeId())) {
          throw new RuntimeException(
              "Nodes don't form a chain. This shouldn't happen and could be " +
              "a bug in the code.");
        }
      }

      // Merge the nodes sequentially.
      GraphNode merged_node = new GraphNode(chain.get(0).node.getNode());
      DNAStrand merged_strand = chain.get(0).compressible_strand;
      for (int pos = 0; pos < chain.size() - 1; pos++) {
        node.setData(chain.get(pos + 1).node.getNode());
        StrandsForEdge strands_for_merge = StrandsUtil.form(
            merged_strand, chain.get(pos + 1).compressible_strand);
        NodeMerger.MergeResult result = NodeMerger.mergeNodes(
            merged_node, node, strands_for_merge, K - 1);

        merged_node = result.node;
        merged_strand = result.strand;
      }

      merged_node.setNodeId(new_id);

      // Now we need to update the incoming edges to the ends of the chain.
      node.setData(chain.get(0).node.getNode());
      List<EdgeUpdateAfterMerge> head_messages = updateMessagesForEdge(
          node, chain.get(0).compressible_strand,
          merged_node.getNodeId(), merged_strand);

      // For the tail node, since updateMessagesForEdge gets the incoming
      // edges, we need to look at the reverse complement for the merged
      // strand
      int tail_pos = chain.size() - 1;
      node.setData(chain.get(tail_pos).node.getNode());
      List<EdgeUpdateAfterMerge> tail_messages = updateMessagesForEdge(
          node, DNAStrandUtil.flip(chain.get(tail_pos).compressible_strand),
          merged_node.getNodeId(), DNAStrandUtil.flip(merged_strand));

      // Clear and set the list of messages in the output array.
      output.getUpdateMessages().clear();
      output.getUpdateMessages().addAll(head_messages);
      output.getUpdateMessages().addAll(tail_messages);

      CompressibleNodeData compressible_node = new CompressibleNodeData();
      compressible_node.setNode(merged_node.getData());
      compressible_node.setCompressibleStrands(
          isCompressible(chain, merged_strand));
      output.setCompressibleNode(compressible_node);
    }

    /**
     * Utility class for storing nodes to be compressed in an array.
     * Each node is stored as ChainLink. The node field stores the actual
     * node. The field compressible_strand stores which strand of the node
     * should be compressed.
     */
    private class ChainLink {
      CompressibleNodeData node;
      DNAStrand compressible_strand;
    }

    /**
     * This function sorts the nodes into a chain that can be compressed.
     * The nodes in the returned chain are ordered such that strand[i]
     * of node[i] has an outgoing edge to strand[i + 1] of node[i + 1].
     * @param nodes
     * @return
     */
    private ArrayList<ChainLink> sortNodes(
        ArrayList<CompressibleNodeData> nodes) {
      HashMap<String, CompressibleNodeData>
        nodes_map = new HashMap<String, CompressibleNodeData>();

      ArrayList<ChainLink> chain = new ArrayList<ChainLink> ();
      for (CompressibleNodeData node: nodes) {
        nodes_map.put(node.getNode().getNodeId().toString(), node);
      }
      GraphNode graph_node = new GraphNode();

      // To sort the nodes we start by finding one end of the chain.
      for (CompressibleNodeData node: nodes) {
        graph_node.setData(node.getNode());
        if (node.getCompressibleStrands() != CompressibleStrands.BOTH) {
          // This node must be one end of the chain because it
          // is compressible along a single direction.
          ChainLink link = new ChainLink();
          link.node = node;
          link.compressible_strand =
              compressibleStrandsToDNAStrand(node.getCompressibleStrands());
          chain.add(link);
          break;
        }
        // Both strands must be compressible.
        TailData f_tail = graph_node.getTail(
            DNAStrand.FORWARD, EdgeDirection.OUTGOING);
        TailData r_tail = graph_node.getTail(
            DNAStrand.REVERSE, EdgeDirection.OUTGOING);

        // If the nodes for both tails are provided then this is a middle
        // node.
        if (nodes_map.containsKey(f_tail.terminal.nodeId) &&
            nodes_map.containsKey(r_tail.terminal.nodeId)) {
          continue;
        }
        ChainLink link = new ChainLink();
        link.node = node;
        if (nodes_map.containsKey(f_tail.terminal.nodeId)) {
          link.compressible_strand = DNAStrand.FORWARD;
        } else {
          link.compressible_strand = DNAStrand.REVERSE;
        }
        chain.add(link);
        break;
      }

      // Chain contains the first node in the chain. So we walk the chain
      // in order to add the other nodes.
      graph_node.setData(chain.get(0).node.getNode());
      TailData tail = graph_node.getTail(
          chain.get(0).compressible_strand, EdgeDirection.OUTGOING);
      while (nodes_map.containsKey(tail.terminal.nodeId)) {
        ChainLink link = new ChainLink();
        link.node = nodes_map.get(tail.terminal.nodeId);
        link.compressible_strand = tail.terminal.strand;
        chain.add(link);
        // Advance to the next node in the chain.
        graph_node.setData(link.node.getNode());
        tail = graph_node.getTail(
            link.compressible_strand, EdgeDirection.OUTGOING);
      }

      // Sanity check. Check the size of the chain is correct.
      if (chain.size() != nodes.size()) {
        throw new RuntimeException(
            "The chain constructed doesn't have all the nodes. This is most " +
            "likely a bug in the code.");
      }
      return chain;
    }

    public void reduce(
        CharSequence nodeid, Iterable<CompressibleNodeData> iterable,
        AvroCollector<PairMergeOutput> collector, Reporter reporter)
            throws IOException {
      Iterator<CompressibleNodeData> iter = iterable.iterator();

      // The nodes to merge.
      ArrayList<CompressibleNodeData> nodes_to_merge =
          new ArrayList<CompressibleNodeData>();
      while(iter.hasNext()) {
        CompressibleNodeData node_data = iter.next();

        // We need to make a copy of the node because iterable
        // will reuse the same instance when next is called.
        // Because of https://issues.apache.org/jira/browse/AVRO-1045 we
        // can't use the Avro methods for copying the data.
        CompressibleNodeData node_copy = new CompressibleNodeData();
        node_copy.setCompressibleStrands(node_data.getCompressibleStrands());
        GraphNode node = new GraphNode(node_data.getNode()).clone();
        node_copy.setNode(node.getData());
        nodes_to_merge.add(node_copy);
      }

      // Sanity check. There should be at most three nodes in nodes_to_merge.
      if (nodes_to_merge.size() > 3) {
        throw new RuntimeException(
            "There are more than two nodes to merge with node: " + nodeid);
      }

      if (nodes_to_merge.size() == 0) {
        throw new RuntimeException(
            "There is no node to output for nodeid: " + nodeid);
      }

      if (nodes_to_merge.size() == 1) {
        // Output the node
        // Clear the list of messages in the output array.
        output.getUpdateMessages().clear();
        output.setCompressibleNode(nodes_to_merge.get(0));
        collector.collect(output);
        return;
      }
      // Sort the nodes into a chain so that we just need to merge
      // each node with its neighbor.
      // Only the nodes at the end of the chain need to be considered to
      // identify nodes whose edges need to be updated because of the merge.
      ArrayList<ChainLink> chain = sortNodes(nodes_to_merge);
      mergeChain(chain, nodeid.toString());

      collector.collect(output);
    }
  }

  /**
   * Get the options required by this stage.
   */
  protected List<Option> getCommandLineOptions() {
    List<Option> options = super.getCommandLineOptions();
    options.addAll(ContrailOptions.getInputOutputPathOptions());

    // Add options specific to this stage.
    options.add(OptionBuilder.withArgName("K").hasArg().withDescription(
        "KMer size [required]").create("K"));

    // Add options specific to this stage.
    options.add(OptionBuilder.withArgName("randseed").hasArg().withDescription(
        "seed for the random number generator [required]").create("randseed"));
    return options;
  }

  @Override
  protected void parseCommandLine(CommandLine line) {
    super.parseCommandLine(line);
    if (line.hasOption("inputpath")) {
      stage_options.put("inputpath", line.getOptionValue("inputpath"));
    }
    if (line.hasOption("outputpath")) {
      stage_options.put("outputpath", line.getOptionValue("outputpath"));
    }
    if (line.hasOption("K")) {
      stage_options.put("K", Long.valueOf(line.getOptionValue("K")));
    }
    if (line.hasOption("randseed")) {
      stage_options.put(
          "randseed", Long.valueOf(line.getOptionValue("randseed")));
    }
  }

  public int run(String[] args) throws Exception {
    sLogger.info("Tool name: PairMergeAvro");
    parseCommandLine(args);
    return run();
  }

  @Override
  protected int run() throws Exception {
    String[] required_args = {"inputpath", "outputpath", "K", "randseed"};
    checkHasOptionsOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    long K = (Long)stage_options.get("K");
    long randseed = (Long)stage_options.get("randseed");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);
    sLogger.info(" - K: " + K);
    sLogger.info(" - randseed: " + randseed);
    JobConf conf = new JobConf(PairMergeAvro.class);
    conf.setJobName("PairMergeAvro " + inputPath + " " + K);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    Pair<CharSequence, CompressibleNodeData> map_output =
        new Pair<CharSequence, CompressibleNodeData>
          ("", new CompressibleNodeData());
    PairMergeOutput reducer_output = new PairMergeOutput();
    AvroJob.setInputSchema(conf, compressible_node.getSchema());
    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
    AvroJob.setOutputSchema(conf, reducer_output.getSchema());

    AvroJob.setMapperClass(conf, PairMergeMapper.class);
    AvroJob.setReducerClass(conf, PairMergeReducer.class);

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
      JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);

      System.out.println("Runtime: " + diff + " s");
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PairMergeAvro(), args);
    System.exit(res);
  }
}
