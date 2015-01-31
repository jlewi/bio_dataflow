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
import contrail.graph.NodeMerger;
import contrail.graph.NodeReverser;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.stages.GraphCounters.CounterName;

/**
 * The second stage for doing parallel compression of linear chains.
 *
 * The input to the mapper is a NodeInfoForMerge. This record contains a
 * CompressibleNodeDataRecord as well as a field strand_to_merge which
 * identifies which strand if any gets merged. If strand_to_merge is set
 * then the mapper gets the outgoing edge for that strand and sends the
 * node to that neighbor by outputting the node keyed by the id of the
 * neighbor. If strand_to_merge is none then the mapper just outputs the node
 * keyed by its id.
 *
 * The reducer merges all nodes keyed by the same id. At most, 3 nodes
 * should be merged by the reducer; 2 up nodes and 1 down node. The down
 * node is identifiable because its node id will match the reducer key and
 * strand_to_merge will be set to None. The nodes are merged such that
 * the forward strand of the down node always corresponds to the forward
 * strand of the merged node (see the javadoc for PairMarkAvro).
 */
public class PairMergeAvro extends MRStage {
  private static final Logger sLogger = Logger.getLogger(PairMergeAvro.class);


  // The number of nodes which still need to be compressed after PairMerge
  // runs.
  public static CounterName NUM_REMAINING_COMPRESSIBLE =
      new CounterName("Contrail", "nodes_left_to_compress");

  protected static class PairMergeMapper extends
      AvroMapper<NodeInfoForMerge, Pair<CharSequence, NodeInfoForMerge>> {
    private GraphNode node;
    private Pair<CharSequence, NodeInfoForMerge> out_pair;
    @Override
    public void configure(JobConf job) {
      node = new GraphNode();
      out_pair = new Pair<CharSequence, NodeInfoForMerge>(
          "", new NodeInfoForMerge());
    }

    @Override
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
      collector.collect(out_pair);
    }
  }

  protected static class PairMergeReducer extends
    AvroReducer <CharSequence, NodeInfoForMerge, CompressibleNodeData> {
    // The length of the KMers.
    private int K;
    private NodeReverser node_reverser;
    private CompressibleNodeData output;
    private NodeMerger nodeMerger;
    @Override
    public void configure(JobConf job) {
      PairMergeAvro stage = new PairMergeAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K")).parseJobConf(job);
      node_reverser = new NodeReverser();
      output = new CompressibleNodeData();
      nodeMerger = new NodeMerger();
    }

    /**
     * Determines whether the merged node resulting from chain is further
     * compressible.
     * This function assumes the chain doesn't form a cycle so the caller
     * should verify the chain isn't a cycle.
     * @param chain: The chain of nodes which are merged together.
     *   The forward strand of the merged node always corresponds to the
     *   forward strand of the down node. Furthermore, the nodes in
     *   chain are aligned such that the forward strand of the down node
     *   is always merged. Thus, the forward strand of the merged node
     *   corresponds to the strands to merge in chain.
     * @return: Which strands if any of the merged node are compressible.
     */
    protected CompressibleStrands isCompressible(Chain chain) {
      // We need to determine whether the merged node is compressible.
      // For each node at the end of the chain, we can compress the merged
      // node along one strand if that end of the chain was
      // compressible in both directions.
      ArrayList<DNAStrand> compressible_strands = new ArrayList<DNAStrand>();

      if (chain.get(0).node.getCompressibleStrands() ==
          CompressibleStrands.BOTH) {
        compressible_strands.add(DNAStrand.REVERSE);
      }

      int tail = chain.size() - 1;
      if (chain.get(tail).node.getCompressibleStrands() ==
          CompressibleStrands.BOTH) {
        compressible_strands.add(DNAStrand.FORWARD);
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
     * Merge the nodes together.
     *
     * @param chain: An array of nodes to merge together. The nodes
     *   should be ordered corresponding to their edges. In particular
     *   chain[i] should have an outgoing edge for strand chain[i].merge_strand
     *   to chain[i+1]. Furthermore, the nodes should be arranged such that
     *   merge_strand is always FORWARD for the down node. This ensures
     *   the merged strands in chain correspond to the FORWARD strand of chain.
     *
     * @return: The merged node. The forward strand of this node corresponds
     *   to the merged strands of each node.
     */
    protected GraphNode mergeChain(Chain chain) {
      ArrayList<EdgeTerminal> terminalChain = new ArrayList<EdgeTerminal>();
      HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
      for (int pos =0; pos < chain.size(); ++pos) {
        ChainLink link = chain.get(pos);
        String nodeId = link.node.getNode().getNodeId().toString();
        terminalChain.add(new EdgeTerminal(nodeId, link.merge_strand));
        nodes.put(nodeId, new GraphNode(link.node.getNode()));
      }

      String downNodeId =
          chain.get(chain.down_index).node.getNode().getNodeId().toString();
      NodeMerger.MergeResult result = nodeMerger.mergeNodes(
          downNodeId, terminalChain, nodes, K - 1);

      // We need the sequence stored in the node to represent the sequence
      // corresponding to the down node. So we check if the merged_strand
      // corresponds to the strand of the down_node that was merged.
      // If not we reverse the node.
      DNAStrand downMergedStrand = chain.get(chain.down_index).merge_strand;

      GraphNode mergedNode = result.node;
      if (result.strand != downMergedStrand) {
        mergedNode = node_reverser.reverse(mergedNode);
      }

      return mergedNode;
    }

    /**
     * Utility class for storing nodes to be compressed in an array.
     * Each node is stored as ChainLink. The node field stores the actual
     * node. The field merge_strand stores which strand of the node
     * should be merged.
     */
    private class ChainLink {
      public ChainLink(CompressibleNodeData node, DNAStrand strand) {
        this.node = node;
        this.merge_strand = strand;
      }

      CompressibleNodeData node;
      DNAStrand merge_strand;
    }

    /**
     * A chain of nodes to merge.
     */
    @SuppressWarnings("serial")
    private class Chain extends ArrayList<ChainLink> {
      public Chain () {
        down_index = -1;
      }
      // Which element in the chain corresponds to the down node.
      public int down_index;
    }

    /**
     * This function sorts the nodes into a chain that can be compressed.
     * The nodes in the returned chain are ordered such that strand[i]
     * of node[i] has an outgoing edge to strand[i + 1] of node[i + 1].
     *
     * Furthermore, the nodes are arranged so the DOWN node is always merged
     * along its FORWARD strand. This ensures that the merged strands of
     * the returned chain correspond to the forward strand of the merged node.
     *
     * The node assigned a state of DOWN is identifiable because
     * strand_to_merge will be set to none.
     * @param nodes
     * @return
     */
    private Chain sortNodes(ArrayList<NodeInfoForMerge> nodes) {
      List<NodeInfoForMerge> up_nodes = new ArrayList<NodeInfoForMerge>();

      // Loop through the nodes and find the down nodes.
      // Then order the nodes in the chain based on the down node.
      String down_id = null;

      Chain chain = new Chain();

      for (NodeInfoForMerge node: nodes) {
        String node_id =
            node.getCompressibleNode().getNode().getNodeId().toString();
        if (node.getStrandToMerge() == CompressibleStrands.NONE) {
          // Sanity check a single node should ha strandToMerge None.
          if (down_id != null) {
            throw new RuntimeException(
                "More than 1 node has strand_to_merge set to none.");
          }
          down_id = node_id;
          // Always merge the forward strand of the down node.
          ChainLink link = new ChainLink(
              node.getCompressibleNode(), DNAStrand.FORWARD);
          chain.add(link);
          chain.down_index = 0;
        } else {
          up_nodes.add(node);
        }
      }
      GraphNode graph_node = new GraphNode();

      // To sort the nodes we process each up node to determine its
      // position relative to the down node.
      for (NodeInfoForMerge up_node: up_nodes) {
        DNAStrand strand_to_merge =
            CompressUtil.compressibleStrandsToDNAStrand(
                up_node.getStrandToMerge());
        graph_node.setData(up_node.getCompressibleNode().getNode());
        List<EdgeTerminal> terminals = graph_node.getEdgeTerminals(
            strand_to_merge, EdgeDirection.OUTGOING);
        // Sanity check there should be a single edge.
        if (terminals.size() != 1) {
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
          ++chain.down_index;
        } else {
          // Since this up node is merged with the reverse strand of
          // the down node, we need to take the reverse of strand to merge
          // to get the strand of this node that is merged with the
          // forward strand of the down node.
          ChainLink link = new ChainLink(
              up_node.getCompressibleNode(),
              DNAStrandUtil.flip(strand_to_merge));

          // Insert this node after the down node
          chain.add(chain.down_index + 1, link);
        }
      }

      // Sanity check. Check the size of the chain is correct.
      if (chain.size() != nodes.size()) {
        throw new RuntimeException(
            "The chain constructed doesn't have all the nodes. This is most " +
            "likely a bug in the code.");
      }
      if (chain.down_index < 0) {
        throw new RuntimeException(
            "The chain constructed doesn't have a down node. This is most " +
            "likely a bug in the code.");
      }
      return chain;
    }

    @Override
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

        // We need to make a copy of the node because iterable reuses the
        // data.
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
        CompressibleNodeData node = nodes_to_merge.get(0).getCompressibleNode();
        if (node.getCompressibleStrands() != CompressibleStrands.NONE) {
          reporter.incrCounter(
              NUM_REMAINING_COMPRESSIBLE.group, NUM_REMAINING_COMPRESSIBLE.tag,
              1);
          reporter.incrCounter("PairMergeAvro", "nodes-unmerged", 1);
        }
        // Output the node
        collector.collect(node);
        return;
      }

      reporter.incrCounter(
          "PairMergeAvro", "nodes-merged", nodes_to_merge.size());

      // Sort the nodes into a chain so that we just need to merge
      // each node with its neighbor.
      Chain chain = sortNodes(nodes_to_merge);
      GraphNode merged_node = mergeChain(chain);

      CompressibleStrands compressible_strands;
      // Check if the merged_node is connected to itself. isCompressible
      // can't handle this.
      if (merged_node.hasSelfCycle()) {
        // We have a cycle so it isn't further compressible.
        compressible_strands = CompressibleStrands.NONE;
      } else {
        compressible_strands = isCompressible(chain);
      }

      if (compressible_strands != CompressibleStrands.NONE) {
        reporter.incrCounter(
            NUM_REMAINING_COMPRESSIBLE.group, NUM_REMAINING_COMPRESSIBLE.tag,
            1);

      }
      output.setNode(merged_node.getData());
      output.setCompressibleStrands(compressible_strands);
      collector.collect(output);
    }
  }

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

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    NodeInfoForMerge merge_info = new NodeInfoForMerge();
    Pair<CharSequence, NodeInfoForMerge> map_output =
        new Pair<CharSequence, NodeInfoForMerge> ("", merge_info);

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    AvroJob.setInputSchema(conf, merge_info.getSchema());
    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
    AvroJob.setOutputSchema(conf, compressible_node.getSchema());

    AvroJob.setMapperClass(conf, PairMergeMapper.class);
    AvroJob.setReducerClass(conf, PairMergeReducer.class);
  }

  @Override
  protected void postRunHook() {
    try {
      long numCompressibleRemaining = job.getCounters().findCounter(
          NUM_REMAINING_COMPRESSIBLE.group,
          NUM_REMAINING_COMPRESSIBLE.tag).getValue();
      long numNodes = job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "REDUCE_OUTPUT_RECORDS").getValue();
      sLogger.info(
          "Number of remaining nodes to compress:" + numCompressibleRemaining);
      sLogger.info("Number of nodes in graph:" + numNodes);
    } catch (IOException e) {
      sLogger.fatal("Couldn't get counters.", e);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PairMergeAvro(), args);
    System.exit(res);
  }
}
