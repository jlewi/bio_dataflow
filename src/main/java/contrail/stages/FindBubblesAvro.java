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

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 *   Sequencing errors in the middle of reads creates "Bubbles"
 *   After removing Tips we look for the Bubble formations in the graph;
 *   This is done in 2 steps:
 *   1. FindBubblesAvro finds these potential bubbles
 *   2. PopBubblesAvro removes (or degenerates) a bubble into a linear chain
 *
 *   Nomenclature: X->{A,B}->Y refers to graph X->A; X->B; B->Y; A->Y
 *   this is the formation of the simplest Bubble
 *   If the sequences for A and B are sufficiently similar, these sequences are merged together into node A forming a chain X->A->Y.
 *   FindBubbles looks for the bubbles in the graph; i.e. those nodes
 *   1. that have indegree and outdegree exactly 1
 *   2. their Sequence length is less than some threshold (let this threshold value be bubbleLenThresh)
 *   3. Sequences in A and B are sufficiently similar if their edit distance is under some THRESHOLD
 *
 *   The Mapper does the following:
 *      -- If the forward degree and reverse degree is = 1, then we may have a potential bubble in the node.
 *         (in example X->{A,B}->Y; A,B are identified as potential bubbles as they have indegree==outdegree=1)
 *      -- We find out terminal nodes of the Incoming and Outgoing edge direction of such nodes (referred to as major and minor nodes);
 *      -- We keep major's ID to be strictly (lexicographically) more than the minor's ID.
 *         (assume here major(A) == major(B) that is node Y and minor(A) == minor(B) = node X)
 *      -- Finally we emit majorID of node as key; and bubble NodeData as value
 *
 *   the Reducer
 *      -- collects node with same majorID
 *	       (therefore node A and node B are sent to same reducer)
 *      -- stores the emitted details in a stage-specific helper class (BubbleMetaData)
 *      -- characterizes BubbleMetaData of nodes according to a List; (as minor(A) == minor(B) they are in same list)
 *      -- recognize if potential Bubble can be removed by calculating the edit distances among two nodes with same minorID
 *         (i.e. how similar sequences of A and B are; can we afford to remove one of them ?)
 *      -- add Node-to-be-Removed ID, Node-to-be-saved ID and updated-Coverage to node_data for PopBubbles stage
 *      -- disconnect them from major Node (and delete major strand)
 *         (here suppose B gets disconnected from Y then,
 *         B is the dead node and A is alive node)
 */

public class FindBubblesAvro extends Stage   {
  private static final Logger sLogger = Logger.getLogger(FindBubblesAvro.class);

  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(Schema.create(Schema.Type.STRING),
          (new GraphNodeData()).getSchema());

  private static Pair<CharSequence, GraphNodeData> out_pair =
      new Pair<CharSequence, GraphNodeData>(MAP_OUT_SCHEMA);

  public static final Schema REDUCE_OUT_SCHEMA =
      new FindBubblesOutput().getSchema();

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    // Add options specific to this stage.
    ParameterDefinition bubble_edit_rate =
        new ParameterDefinition("bubble_edit_rate",
            "bubble editing rate", Integer.class, new Integer(0));
    ParameterDefinition bubble_length_threshold =
        new ParameterDefinition("bubble_length_threshold",
            "A threshold for sequence lengths. Only sequence's with lengths less " +
                "than this value will be detected as potential bubble", Integer.class, new Integer(0));

    for (ParameterDefinition def:
      new ParameterDefinition[] {
        bubble_length_threshold, bubble_edit_rate}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  //FindBubblesMapper
  ///////////////////////////////////////////////////////////////////////////
  public static class FindBubblesAvroMapper extends
  AvroMapper<GraphNodeData, Pair<CharSequence,GraphNodeData>>    {
    int bubbleLenThresh = 0;
    GraphNode node = null;
    GraphNodeData msg = null;

    public void configure(JobConf job)    {
      FindBubblesAvro stage= new FindBubblesAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      bubbleLenThresh = (Integer)(definitions.get("bubble_length_threshold").parseJobConf(job));
      // the mapper message
      msg= new GraphNodeData();
      node= new GraphNode();
    }

    public void map(GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, GraphNodeData>> output,
        Reporter reporter) throws IOException   {
      node.setData(graph_data);

      int nodeLength = graph_data.getSequence().getLength();
      int outDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING);
      int inDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING);
      // check if node can't be a bubble
      if(  (nodeLength >= bubbleLenThresh) || ((outDegree != 1) || (inDegree != 1)) )  {
        msg = node.getData();
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);
        return;
      }
      /**
       *  Here we just work with one strand of DNA that is, the FORWARD strand
       *  we get the degrees of FORWARD DNAStrand with respect to to Incoming and Outgoing edges and
       *  try to identify if it is a bubble
       *  then we get terminal IDs of both directions of the Forward Strand
       *  and determine minorID and majorID on the basis of their lexicographic ordering
       */
      EdgeTerminal in = null;
      EdgeTerminal out = null;
      reporter.incrCounter("Contrail", "potential-bubbles", 1);
      TailData out_tail = node.getTail(DNAStrand.FORWARD, EdgeDirection.OUTGOING);
      TailData in_tail = node.getTail(DNAStrand.FORWARD, EdgeDirection.INCOMING);
      out = out_tail.terminal;
      in = in_tail.terminal;
      CharSequence major = "";

      // we ship the potential bubble to its neighbor with the lexicographically larger id (majorId).
      // This ensures the nodes forming a bubble get shipped to the same node.
      major = GraphUtil.computeMajorID(in.nodeId, out.nodeId);
      GraphNodeData msg = node.getData();
      out_pair.set(major, msg);
      output.collect(out_pair);
    }
  }

  // FindBubblesReducer
  ///////////////////////////////////////////////////////////////////////////
  public static class FindBubblesAvroReducer
  extends AvroReducer<CharSequence, GraphNodeData, FindBubblesOutput> {
    private int K;
    public float bubbleEditRate;
    private GraphNode actual_node = null;
    private GraphNode bubble_node = null;
    private FindBubblesOutput output = null;

    public void configure(JobConf job) {
      FindBubblesAvro stage = new FindBubblesAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();

      bubbleEditRate= (Integer)(definitions.get("bubble_edit_rate").parseJobConf(job));
      K = (Integer)(definitions.get("K").parseJobConf(job));
      actual_node = new GraphNode();
      bubble_node = new GraphNode();
      output = new FindBubblesOutput();
      output.setNodeBubbleinfo(new ArrayList<BubbleInfo>());
    }

    /**
     * This class is used to store and compare Potential bubbles emitted from the mapper
     * on the base of their coverage values
     */
    private class BubbleMetaData implements Comparable<BubbleMetaData>   {
      // The Bubble node
      GraphNode node;
      // extraCoverage needed to be updated; once a bubble gets popped
      float extraCoverage;
      // a boolean variable used for marking bubbles
      boolean popped;
      // the corresponding node which has higher coverage when compared with Bubble
      CharSequence aliveNodeID;
      // Aligned Sequence
      Sequence alignedSequence;
      // Minor ID
      CharSequence minor;

      BubbleMetaData(GraphNodeData nodeData, CharSequence major)    {
        node = new GraphNode();
        node.setData(nodeData);
        extraCoverage = 0;
        popped = false;
        aliveNodeID = "";

       // strand type from Major Node
        DNAStrand strandFromMajor;
        if (node.getEdgeTerminals(DNAStrand.FORWARD, EdgeDirection.INCOMING).
            get(0).nodeId.toString().equals(major.toString())){
          strandFromMajor = DNAStrand.FORWARD;
        } else {
          strandFromMajor = DNAStrand.REVERSE;
        }
        alignedSequence = DNAUtil.sequenceToDir(node.getSequence(), strandFromMajor);

        // get minor for bubbleNode
        if(!(node.getTail(DNAStrand.FORWARD,
            EdgeDirection.OUTGOING).terminal.nodeId.toString().equals(major.toString())) )  {
          minor = node.getTail(DNAStrand.FORWARD, EdgeDirection.OUTGOING).terminal.nodeId;
        }
        else  {
          minor = node.getTail(DNAStrand.FORWARD, EdgeDirection.INCOMING).terminal.nodeId;
        }
      }

      /**
       * A positive result means coverage(this) < coverage(other). Conversely, a negative
       * result means coverage(this) > coverage(other)
       */
      public int compareTo(BubbleMetaData o)    {
        BubbleMetaData co = o;
        return (int)(co.node.getCoverage() - node.getCoverage());
      }
    }

    /**
     *  For every pair of nodes (u, v) we compute the edit distance between the sequences.
     *  If the edit distance is less than edit_distance_threshold then the
     *  sequences are sufficiently similar and we can mark the node v to be removed
     *  where node v is the node with smaller coverage.
     */
    protected void ProcessMinorList(List<BubbleMetaData> minor_list, Reporter reporter, CharSequence minor)  {
      // Sort potential bubble strings in order of decreasing coverage
      Collections.sort(minor_list);

      for (int i = 0; i < minor_list.size(); i++)   {
        BubbleMetaData highCoverageNode = minor_list.get(i);
        if(highCoverageNode.popped)  {
          continue;
        }
        for (int j = i+1; j < minor_list.size(); j++)   {
          BubbleMetaData lowCoverageNode = minor_list.get(j);
          if(lowCoverageNode.popped)  {
            continue;
          }

          int distance = highCoverageNode.alignedSequence.computeEditDistance(lowCoverageNode.alignedSequence);
          int threshold = (int) Math.max(highCoverageNode.node.getSequence().size(),
              lowCoverageNode.node.getSequence().size() * bubbleEditRate);
          reporter.incrCounter("Contrail", "bubbleschecked", 1);

          if (distance <= threshold)  {
            // Found a bubble!
            reporter.incrCounter("Contrail", "poppedbubbles", 1);
            int dead_seq_merlen = lowCoverageNode.node.getSequence().size()- K + 1;
            // since lowCoverageNode is the bubble that will be popped; we need to update coverage of highCoverageNode
            float extracov =  lowCoverageNode.node.getCoverage() * dead_seq_merlen;
            lowCoverageNode.popped = true;
            actual_node.removeNeighbor(lowCoverageNode.node.getNodeId());
            // store data for popBubbles; for illustration mentioned start; in graph X->{A,B}->Y
            lowCoverageNode.extraCoverage = extracov;
            lowCoverageNode.aliveNodeID = highCoverageNode.node.getNodeId();
          }
        }
      }
    }

    // Output the messages to the minor node.
    void outputMessagesToMinor(List<BubbleMetaData> minor_list, CharSequence minor,
        AvroCollector<FindBubblesOutput> collector) throws IOException {

      output.setNode(null);
      output.getNodeBubbleinfo().clear();

      ArrayList<BubbleInfo> minorMessages = new ArrayList<BubbleInfo>();

      for(BubbleMetaData bubbleMetaData : minor_list) {
        BubbleInfo bubble = new BubbleInfo();
        //GraphNodeData nodeData = null;

        if(bubbleMetaData.popped) {
          bubble.setNodetoRemoveID(bubbleMetaData.node.getNodeId());
          bubble.setExtraCoverage(bubbleMetaData.extraCoverage);
          bubble.setTargetID(minor);
          bubble.setAliveNodeID(bubbleMetaData.aliveNodeID);
          minorMessages.add(bubble);
        } else {
          // This is a non-popped node so output the node.
          output.setNode(bubbleMetaData.node.getData());
          collector.collect(output);
//          // initialize non null values
//          ArrayList<BubbleInfo> empty_list = new ArrayList<BubbleInfo>();
//          bubble.setNodetoRemoveID("");
//          bubble.setExtraCoverage((float) 0);
//          bubble.setTargetID("");
//          bubble.setAliveNodeID("");
//          empty_list.add(bubble);
//
//          nodeData = bubbleMetaData.node.getData();
//          reducer_msg.setNode(nodeData);
//          reducer_msg.setNodeBubbleinfo(empty_list);
//          output.collect(reducer_msg);
        }
      }
      // Output the messages to the minor node.
      output.setNode(null);
      output.setNodeBubbleinfo(minorMessages);
      collector.collect(output);
    }

    public void reduce(CharSequence nodeid, Iterable<GraphNodeData> iterable,
        AvroCollector< FindBubblesOutput > collector, Reporter reporter)
            throws IOException    {
      // this hashMap will have list of BubbleMetaData of all nodes that correspond to a particular node as their minor (EdgeTerminal)
      Map<String, List<BubbleMetaData>> bubblelinks = new HashMap<String, List<BubbleMetaData>>();
      int sawnode = 0;
      Iterator<GraphNodeData> iter = iterable.iterator();
      ArrayList<BubbleInfo> bubble_info_list = new ArrayList<BubbleInfo>();

      while(iter.hasNext())   {
        GraphNodeData msg = iter.next();

        // this is a normal node: only node data is set in schema
        if (msg.getNodeId().equals(nodeid))    {
          actual_node.setData(msg);
          actual_node= actual_node.clone();
          sawnode++;
        }
        else    {
          bubble_node.setData(msg);
          bubble_node= bubble_node.clone();

          BubbleMetaData bubble_message = new BubbleMetaData(bubble_node.getData(), nodeid);

          reporter.incrCounter("Contrail", "linkschecked", 1);
          // see if the hashmap has that particular ID of edge terminal; if not then add it
          if (!bubblelinks.containsKey(bubble_message.minor.toString()))    {
            List<BubbleMetaData> blist = new ArrayList<BubbleMetaData>();
            bubblelinks.put((String) bubble_message.minor.toString(), blist);
          }
          bubblelinks.get(bubble_message.minor.toString()).add(bubble_message);
        }
      }

      if (bubblelinks.size() > 0)   {
        for (String minorID : bubblelinks.keySet())   {

          List<BubbleMetaData> minor_list = bubblelinks.get(minorID);
          int choices = minor_list.size();
          reporter.incrCounter("Contrail", "minorchecked", 1);
          if (choices <= 1)    {
            continue;
          }
          reporter.incrCounter("Contrail", "edgeschecked", choices);
          // marks nodes to be deleted for a particular list of minorID
          ProcessMinorList(minor_list, reporter, minorID);
          outputMessagesToMinor(minor_list, minorID, collector);
        }
      }

      // Output the major node.
      output.setNode(actual_node.getData());
      output.getNodeBubbleinfo().clear();
      collector.collect(output);
      if (sawnode != 1)    {
        throw new IOException("ERROR: Saw multiple nodemsg (" + sawnode + ") for " + nodeid.toString());
      }
    }
  }

  // Run Tool
  ///////////////////////////////////////////////////////////////////////////

  public RunningJob runJob() throws Exception
  {
    String[] required_args = {"inputpath", "outputpath","bubble_edit_rate","bubble_length_threshold"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    int K = (Integer)stage_options.get("K");

    sLogger.info("Tool name: FindBubbles");
    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    }
    else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("FindBubbles " + inputPath + " " + K);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, FindBubblesAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, FindBubblesAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, FindBubblesAvroMapper.class);
    AvroJob.setReducerClass(conf, FindBubblesAvroReducer.class);

    //delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    long starttime = System.currentTimeMillis();
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();

    float diff = (float) ((endtime - starttime) / 1000.0);

    sLogger.info("Runtime: " + diff + " s");
    return result;
  }

  // Main
  ///////////////////////////////////////////////////////////////////////////

  public static void main(String[] args) throws Exception   {
    int res = ToolRunner.run(new Configuration(), new FindBubblesAvro(), args);
    System.exit(res);
  }
}
