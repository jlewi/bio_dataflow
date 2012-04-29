package contrail.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
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
 * For each node we randomly flip a coin and assign the node a value of Up
 * or Down. The random seed is based on a global seed value and the node id.
 * Thus, each node can compute the result of the coin toss for any other node.
 *
 * Consider Two nodes A->B. Suppose A has a single outgoing edge to B and
 * B has a single incoming edge from A. In this case, nodes A and B can be
 * merged. However, these two nodes will be merged during the merge phase
 * (PairMerge) only if we mark them to be merge.
 *
 * We mark A and B to be merged in the following cases.
 *
 * Suppose the coin toss for A is Up and Down for B.
 *
 */
public class PairMergeAvro extends Stage {
  private static final Logger sLogger = Logger.getLogger(PairMergeAvro.class);

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
  AvroMapper<CompressibleNodeData, Pair<CharSequence, MergeNodeData>> {
    private CompressibleNode node;
    private MergeNodeData output;
    private CoinFlipper flipper;
    public void configure(JobConf job) {
      node = new CompressibleNode();
      output = new MergeNodeData();
      flipper = new CoinFlipper(Long.parseLong(job.get("randseed")));
      out_pair = new Pair<CharSequence, MergeNodeData>("", output);
    }

    public TailData getBuddy(CompressibleNode node, DNAStrand strand) {
      if (node.canCompress(strand)) {
        return node.getNode().getTail(strand, EdgeDirection.OUTGOING);
      }
      return null;
    }

    // Output pair
    private Pair<CharSequence, MergeNodeData> out_pair;

    private void clearOutput () {
      output.setNode(null);
      output.setStrandToMerge(CompressibleStrands.NONE);
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

    // Handle a node which is a assigned Up by the coin toss.
    // Since the node is assigned Up we can merge it into one of its
    // tails if that tails is assigned Down.
    private EdgeToCompress processUpNode(
        CompressibleNode node, TailData fbuddy, TailData rbuddy) {
      // Prefer Merging forward if we can.
      // We can only merge in a single direction at a time.
      if (fbuddy != null) {

        CoinFlipper.CoinFlip f_flip = flipper.flip(fbuddy.terminal.nodeId);

        if (f_flip == CoinFlipper.CoinFlip.Down) {
          // We can compress the forward strand.
          return new EdgeToCompress(fbuddy.terminal, DNAStrand.FORWARD);
        }
      }

      // If we can't compress the forward strand, see if
      // we can compress the reverse strand.
      if (rbuddy != null) {
        CoinFlipper.CoinFlip r_flip = flipper.flip(rbuddy.terminal.nodeId);

        if (r_flip == CoinFlipper.CoinFlip.Down) {
          return new EdgeToCompress(rbuddy.terminal, DNAStrand.REVERSE);
        }
      }

      // Can't do a merge.
      // Should we ever be able to reach this point?
      return null;
    }

    // If this node was randomly assigned a state of Down then we can
    // potentially convert it to Up and merge it with some of its neighbors.
    // If a node is assigned Down normally we won't do anything because
    // if its connected to a Up node the Up Node will be sent by the mapper
    // to a Down node its connected to and the Reducer will merge the two
    // nodes. However, suppose we have a sequence of two or more Down nodes
    // in a row. Then normally none of the nodes would get merged.
    // However, we can potentially convert the down node to an Up Node
    // and do a merge.
    private boolean convertDownToUp(
        CompressibleNode node, TailData fbuddy, TailData rbuddy) {

      // This node is a tail.
      if ((rbuddy != null) && (fbuddy != null)) {
        // We have tails for both strands of this node.

        //boolean fmale = isMale(fbuddy.id);
        //boolean rmale = isMale(rbuddy.id);
        CoinFlipper.CoinFlip f_flip = flipper.flip(fbuddy.terminal.nodeId);
        CoinFlipper.CoinFlip r_flip = flipper.flip(rbuddy.terminal.nodeId);

        if ( f_flip == CoinFlipper.CoinFlip.Down &&
            r_flip == CoinFlipper.CoinFlip.Down &&
            (node.getNode().getNodeId().compareTo(
                fbuddy.terminal.nodeId) < 0) &&
                (node.getNode().getNodeId().compareTo(
                    rbuddy.terminal.nodeId) < 0)) {

          return true;
        }
        return false;
      }

      if (rbuddy == null) {
        // We only have a tail for the forward strand but not the reverse
        // strand.
        //boolean fmale = isMale(fbuddy.id);
        CoinFlipper.CoinFlip f_flip = flipper.flip(fbuddy.terminal.nodeId);
        if (f_flip == CoinFlipper.CoinFlip.Down && (
            node.getNode().getNodeId().compareTo(
                fbuddy.terminal.nodeId) < 0)) {

          return true;
        }
        return false;
      }

      if (fbuddy == null) {
        //boolean rmale = isMale(rbuddy.id);

        CoinFlipper.CoinFlip r_flip = flipper.flip(rbuddy.terminal.nodeId);

        if (r_flip == CoinFlipper.CoinFlip.Down && (
            node.getNode().getNodeId().compareTo(
                rbuddy.terminal.nodeId) < 0)) {
          return true;
        }
        return false;
      }
      return false;
    }

    public void map(CompressibleNodeData node_data,
        AvroCollector<Pair<CharSequence, MergeNodeData>> collector,
        Reporter reporter) throws IOException {
      node.setData(node_data);

      // Check if either the forward or reverse strand can be merged.
      TailData fbuddy = getBuddy(node, DNAStrand.FORWARD);
      TailData rbuddy = getBuddy(node, DNAStrand.REVERSE);

      if (fbuddy == null && rbuddy == null) {
        // Node can't be compressed so output the node and we are done.
        clearOutput();
        output.setNode(node.getNode().getData());
        out_pair.key(node_data.getNode().getNodeId());
        out_pair.value(output);
        collector.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
        return;
      }

      reporter.incrCounter("Contrail", "compressible", 1);

      CoinFlipper.CoinFlip coin = flipper.flip(node.getNode().getNodeId());

      // If this node is randomly assigned Down, see if it can be converted
      // to up.
      if (coin == CoinFlipper.CoinFlip.Down) {
        if (convertDownToUp(node, fbuddy, rbuddy)) {
          coin = CoinFlipper.CoinFlip.Up;
        }
      }

      if (coin == CoinFlipper.CoinFlip.Down) {
        // Just output this node since this is a Down node
        // any node to be merged with this node will be sent to this node.
        clearOutput();
        output.setNode(node.getNode().getData());
        out_pair.key(node_data.getNode().getNodeId());
        out_pair.value(output);
        collector.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
        return;
      }

      // Check if this node can be sent to one of its neighbors to be merged.
      EdgeToCompress edge_to_compress =
          processUpNode(node, fbuddy, rbuddy);


      if (edge_to_compress == null) {
        // This node doesn't get sent to another node be merged.
        clearOutput();
        output.setNode(node.getNode().getData());
        out_pair.key(node_data.getNode().getNodeId());
        out_pair.value(output);
        collector.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
        return;
      }


      // The key for the output is the id for the node this node gets merged
      // into
      clearOutput();

      output.setNode(node.getNode().getData());
      if (edge_to_compress.strand == DNAStrand.FORWARD) {
        output.setStrandToMerge(CompressibleStrands.FORWARD);
      } else {
        output.setStrandToMerge(CompressibleStrands.REVERSE);
      }
      out_pair.key(edge_to_compress.other_terminal.nodeId);
      out_pair.value(output);
      collector.collect(out_pair);
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
    AvroReducer <CharSequence, MergeNodeData, PairMergeOutput> {

    private PairMergeOutput output;
    private int K;

    public void configure(JobConf job) {
      K = Integer.parseInt(job.get("K"));
      output = new PairMergeOutput();
      output.setUpdateMessages(new ArrayList<EdgeUpdateAfterMerge>());
      output.setNode(null);
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

    protected List<EdgeUpdateAfterMerge> updateMessagesForEdge(
        GraphNode node, DNAStrand strand, String old_id, String new_nodeid,
        DNAStrand new_strand) {
      // Now we need to form messages to update the incoming and outgoing
      // edges. We need to check if the message is to one of the nodes
      // that we've already sent a message to.

      List<EdgeUpdateAfterMerge> edge_updates =
          new ArrayList<EdgeUpdateAfterMerge> ();
      // For the source node, we need to update the incoming edges
      // to the strand that was merged.
      // and send them a message with the new id and strand for that edge.
      List<EdgeTerminal> incoming_terminals =
          node.getEdgeTerminals(strand, EdgeDirection.INCOMING);

      for (EdgeTerminal terminal: incoming_terminals) {
        EdgeUpdateAfterMerge update = new EdgeUpdateAfterMerge();
        update.setOldStrands(StrandsUtil.form(terminal.strand, strand));
        update.setNewStrands(StrandsUtil.form(terminal.strand, new_strand));

        update.setOldTerminalId(old_id);
        update.setNewTerminalId(new_nodeid);

        update.setNodeToUpdate(node.getNodeId());

        edge_updates.add(update);
      }
      return edge_updates;
    }


    /**
     * Merge the nodes together. The result of the merge is stored in the
     * member variable output.
     *
     * @param chain: An array of nodes to merge together. The nodes
     *   should be ordered corresponding to their position in the chain.
     * @param start_strand: Which strand of the first node in chain to start
     *   on; i.e this strand must have an outgoing edge to the next node
     *   in the chian.
     * @param new_id: The new id to assign to the merged nodes.
     */
    protected void mergeChain(
        ArrayList<GraphNode> chain, DNAStrand start_strand, String new_id) {
      // The nodes in chain should form a chain, such that
      // starting with start_strand we can walk to the last node in chain.
      ArrayList<DNAStrand> strands = new ArrayList<DNAStrand>();
      strands.add(start_strand);

      // Check the chain and find out which strand of each node belongs in
      // the chain.
      for (int pos = 0; pos < chain.size() -1; pos++) {
        GraphNode node = chain.get(pos);
        TailData tail =
            node.getTail(strands.get(pos), EdgeDirection.OUTGOING);
        if (tail == null) {
          throw new RuntimeException(
              "Nodes don't form a chain. This shouldn't happen and could be " +
              "a bug in the code.");
        }
        if (!tail.terminal.nodeId.equals(chain.get(pos + 1).getNodeId())) {
          throw new RuntimeException(
              "Nodes don't form a chain. This shouldn't happen and could be " +
              "a bug in the code.");
        }
        strands.add(pos + 1, tail.terminal.strand);
      }


      // Merge the nodes sequentially.
      GraphNode merged_node = chain.get(0);
      DNAStrand merged_strand = strands.get(0);
      for (int pos = 0; pos < chain.size() -1; pos++) {
        StrandsForEdge strands_for_merge = StrandsUtil.form(
            merged_strand, strands.get(pos + 1));
        NodeMerger.MergeResult result = NodeMerger.mergeNodes(
            merged_node, chain.get(pos + 1), strands_for_merge, K - 1);

        merged_node = result.node;
        merged_strand = result.strand;
      }

      merged_node.setNodeId(new_id);

      // Now we need to update the incoming edges to the ends of the chain.
      List<EdgeUpdateAfterMerge> head_messages = updateMessagesForEdge(
          chain.get(0), strands.get(0), chain.get(0).getNodeId(),
          merged_node.getNodeId(), merged_strand);

      // For the tail node, since updateMessagesForEdge gets the incoming
      // edges, we need to look at the reverse complement for the merged
      // strand
      int tail_pos = chain.size() - 1;
      List<EdgeUpdateAfterMerge> tail_messages = updateMessagesForEdge(
          chain.get(tail_pos), DNAStrandUtil.flip(strands.get(tail_pos)),
          chain.get(0).getNodeId(), merged_node.getNodeId(),
          DNAStrandUtil.flip(merged_strand));

      // Clear the list of messages in the output array.
      output.getUpdateMessages().clear();
      output.getUpdateMessages().addAll(head_messages);
      output.getUpdateMessages().addAll(tail_messages);
      output.setNode(merged_node.getData());
    }

    public void reduce(CharSequence nodeid, Iterable<MergeNodeData> iterable,
        AvroCollector<PairMergeOutput> collector, Reporter reporter)
            throws IOException
            {
      //Node node = new Node(nodeid.toString());
      //List<Update> updates = new ArrayList<Update>();

      Iterator<MergeNodeData> iter = iterable.iterator();

      GraphNode down_node = null;

      // The nodes to merge.
      ArrayList<MergeNodeData> nodes_to_merge =
          new ArrayList<MergeNodeData>();
      while(iter.hasNext()) {
        MergeNodeData merge_info = iter.next();

        if (merge_info.getStrandToMerge() == CompressibleStrands.NONE) {
          // This the down node that the up nodes get merged into.
          // There should be only one.
          if (down_node != null) {
            throw new RuntimeException(
                "There is more than 1 message for node: " + nodeid + " " +
                "that has no strands to merge. This should not happen");
          }
          down_node = new GraphNode();
          down_node.setData(merge_info.getNode());

          // Make a copy of the data.
          down_node = down_node.clone();
          continue;
        }

        if (merge_info.getStrandToMerge() == CompressibleStrands.BOTH) {
          throw new RuntimeException(
              "There message for node: " + nodeid + " " +
                  "one of the nodes is compressible along both strands. This " +
              "should not happen");
        }

        // We need to make a copy of the because iterable
        // will reuse the same instance when next is called.
        // Because of https://issues.apache.org/jira/browse/AVRO-1045 we
        // can't use the Avro methods for copying the data.
        GraphNode node = new GraphNode(merge_info.getNode()).clone();
        MergeNodeData data_copy = new MergeNodeData();
        data_copy.setNode(node.getData());
        data_copy.setStrandToMerge(merge_info.getStrandToMerge());
        nodes_to_merge.add(data_copy);
     }

      // Sanity check. There should be at most two nodes in nodes_to_merge.
      if (nodes_to_merge.size() > 2) {
        throw new RuntimeException(
            "There are more than two nodes to merge with node: " + nodeid);
      }

      if (down_node == null) {
        throw new RuntimeException(
            "There is no node to output for nodeid: " + nodeid);
      }

      if (nodes_to_merge.size() == 0) {
        //Output the node
        // Clear the list of messages in the output array.
        output.getUpdateMessages().clear();
        output.setNode(down_node.getData());
        collector.collect(output);
        return;
      }
      // We want to order the nodes in a chain.
      // e.g node1->node2  (if one node to merge).
      // or node1->node2->node3 (if two nodes to merge with node2).
      // This ordering makes it easy to detect which nodes we need
      // to update so they can move their edges to the new merged node.
      // Only the nodes at the end of the chain need to be considered to
      // identify nodes we need to send messages to.
      ArrayList<GraphNode> chain = new ArrayList<GraphNode>();
      chain.add(new GraphNode(nodes_to_merge.get(0).getNode()));
      chain.add(down_node);

      if (nodes_to_merge.size() == 2) {
        chain.add(new GraphNode(nodes_to_merge.get(1).getNode()));
      }

      // Which strand to start the merge on.
      DNAStrand start_strand = compressibleStrandsToDNAStrand(
          nodes_to_merge.get(0).getStrandToMerge());

      mergeChain(chain, start_strand, down_node.getNodeId());

      collector.collect(output);
    }
  }


  public RunningJob run(String inputPath, String outputPath, long randseed) throws Exception
  {
    sLogger.info("Tool name: PairMergeAvro");
    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);
    sLogger.info(" - randseed: " + randseed);

    JobConf conf = new JobConf();
    conf.setJobName("PairMark " + inputPath);

    //ContrailConfig.initializeConfiguration(conf);
    conf.setLong("randseed", randseed);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);

    //		conf.setMapperClass(PairMarkMapper.class);
    //		conf.setReducerClass(PairMarkReducer.class);

    //delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    return JobClient.runJob(conf);
  }


  //	public int run(String[] args) throws Exception
  //	{
  //		String inputPath  = "/Users/mschatz/try/compressible/";
  //		String outputPath = "/users/mschatz/try/mark1/";
  //		long randseed = 123456789;
  //
  //		run(inputPath, outputPath, randseed);
  //
  //		return 0;
  //	}

  public static void main(String[] args) throws Exception
  {
    int res = ToolRunner.run(new Configuration(), new PairMergeAvro(), args);
    System.exit(res);
  }


  @Override
  protected int run() throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }
}
