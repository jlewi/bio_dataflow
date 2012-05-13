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

  protected static class PairMergeMapper extends
  AvroMapper<NodeInfoForMerge, Pair<CharSequence, NodeInfoForMerge>> {
    private GraphNode node;
    private Pair<CharSequence, NodeInfoForMerge> out_pair;
    public void configure(JobConf job) {
      node = new GraphNode();
      out_pair = new Pair<CharSequence, NodeInfoForMerge>(
          "", new NodeInfoForMerge());
    }

    public void map(NodeInfoForMerge node_info,
        AvroCollector<Pair<CharSequence, NodeInfoForMerge>> collector,
        Reporter reporter) throws IOException {
      // Get the id to send this node to.
      CharSequence out_id =
          node_info.getCompressibleNode().getNode().getNodeId();
      if (node_info.getStrandToMerge() != CompressibleStrands.NONE) {
        node.setData(node_info.getCompressibleNode().getNode());
        DNAStrand strand = CompressUtil.compressibleStrandsToDNAStrand(
            node_info.getStrandToMerge());
        EdgeTerminal destination = node.getEdgeTerminals(
            strand, EdgeDirection.OUTGOING).get(0);
        out_id = destination.nodeId;
      }
      out_pair.key(out_id);
      out_pair.value(node_info);
    }
  }

  protected static class PairMergeReducer extends
    AvroReducer <CharSequence, NodeInfoForMerge, CompressibleNodeData> {

    // The length of the KMers.
    private int K;

    public void configure(JobConf job) {
      K = Integer.parseInt(job.get("K"));
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
      throw new RuntimeException("Need to update this code");
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
      throw new RuntimeException("Need to update this code");
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
      public ChainLink(CompressibleNodeData node, DNAStrand strand) {
        this.node = node;
        this.compressible_strand = strand;
      }
      CompressibleNodeData node;
      DNAStrand compressible_strand;
    }

    /**
     * This function sorts the nodes into a chain that can be compressed.
     * The nodes in the returned chain are ordered such that strand[i]
     * of node[i] has an outgoing edge to strand[i + 1] of node[i + 1].
     *
     * The node assigned a state of DOWN is identifiable because
     * strand_to_merge will be set to none.
     * @param nodes
     * @return
     */
    private ArrayList<ChainLink> sortNodes(
        ArrayList<NodeInfoForMerge> nodes) {
      List<NodeInfoForMerge> up_nodes = new ArrayList<NodeInfoForMerge>();

      // Loop through the nodes and find the down nodes.
      // Then order the nodes in the chain based on the down node.
      String down_id = null;
      // Keep track of the position in the chain of the node corresponding
      // to the down node.
      int down_index = -1;
      ArrayList<ChainLink> chain = new ArrayList<ChainLink> ();
      for (NodeInfoForMerge node: nodes) {
        String node_id =
            node.getCompressibleNode().getNode().getNodeId().toString();
        if (node.getStrandToMerge() == CompressibleStrands.NONE) {
          // Sanity check a single node should of strandToMergeNonde
          if (down_id != null) {
            throw new RuntimeException(
                "More than 1 node has strand_to_merge set to none.");
          }
          down_id = node_id;
          // Always merge the forward strand of the down node.
          ChainLink link = new ChainLink(
              node.getCompressibleNode(), DNAStrand.FORWARD);
          chain.add(link);
          down_index = 0;
        } else {
          up_nodes.add(node);
        }
      }
      GraphNode graph_node = new GraphNode();

      // To sort the nodes we process each up node.
      for (NodeInfoForMerge up_node: up_nodes) {
        DNAStrand strand_to_merge =
            CompressUtil.compressibleStrandsToDNAStrand(
                up_node.getStrandToMerge());
        graph_node.setData(up_node.getCompressibleNode().getNode());
        List<EdgeTerminal> terminals = graph_node.getEdgeTerminals(
            strand_to_merge, EdgeDirection.OUTGOING);
        // Sanity check there should be a single edge.
        if (terminals.size() != 0) {
          throw new RuntimeException(
              "Can't merge node:" + graph_node.getNodeId() + " along strand " +
              strand_to_merge + " because there is more than 1 edge.");
        }
        EdgeTerminal other_terminal = terminals.get(0);
        // Sanity check.
        if (!other_terminal.nodeId.equals(down_id)) {
          throw new RuntimeException(
              "The up node isn't connected to the down node along the strand " +
              "to merge");
        }
        if (other_terminal.strand == DNAStrand.FORWARD) {
         // Since this up node is merged with the forward strand of
         // the down node we insert it before the down node.
          ChainLink link = new ChainLink(
              up_node.getCompressibleNode(), strand_to_merge);
          // The down_node should be at position 0.
          chain.add(0, link);
          // Down node is shifted right.
          ++down_index;
        } else {
          // Since this up node is merged with the reverse strand of
          // the up node, we need to take the reverse of strand to merge
          // to get the strand of this node that is merged with the
          // forward strand of the down node.
          ChainLink link = new ChainLink(
              up_node.getCompressibleNode(),
              DNAStrandUtil.flip(strand_to_merge));

          // Insert this node after the down node
          chain.add(down_index + 1, link);
        }
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
        CharSequence nodeid, Iterable<NodeInfoForMerge> iterable,
        AvroCollector<CompressibleNodeData> collector, Reporter reporter)
            throws IOException {
      Iterator<NodeInfoForMerge> iter = iterable.iterator();

      // The nodes to merge.
      ArrayList<NodeInfoForMerge> nodes_to_merge =
          new ArrayList<NodeInfoForMerge>();
      while(iter.hasNext()) {
        NodeInfoForMerge node_data = iter.next();

        // We need to make a copy of the node because iterable
        nodes_to_merge.add(CompressUtil.copyNodeInfoForMerge(node_data));
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
        collector.collect(nodes_to_merge.get(0).getCompressibleNode());
        return;
      }

      // Sort the nodes into a chain so that we just need to merge
      // each node with its neighbor.
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
