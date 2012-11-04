/* Licensed under the Apache License, Version 2.0 (the "License");
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
package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.stages.GraphCounters.CounterName;

/**
 * This stage finds redundant paths created by sequencing errors.
 *
 * Sequencing errors in the middle of reads creates redundant paths. These 
 * redundant paths are found in formation of type X->{A,B}->Y, where X->{A,B}
 * means X has a path to each node in the set {A,B}. Similarly, {A,B}-> Y 
 * means each node in the set {A, B} has a path to node Y.
 * 
 * If the paths for A and B are sufficiently similar, then then we can assume 
 * one of the paths is the result of sequencing errors and only keep the path 
 * with higher coverage.
 * This type of formation X->{A,B}->Y is called a "bubble". However, we also 
 * compare each bubble to the direct paths between major and minor.
 *
 * Popping bubbles is done in two stages.
 *  1. FindBubblesAvro finds these potential bubbles.
 *  2. PopBubblesAvro removes edges in the graph to the deleted nodes.
 *
 * FindBubbles looks for the bubbles in the graph; i.e. those nodes
 *  1. that have indegree=1 and outdegree=1,
 *  2. their Sequence length is less than some threshold,
 *  3. the edit distance between the sequences for nodes A and B are less than
 *     some THRESHOLD.
 *
 * The Mapper finds potential bubbles (e.g nodes A & B) by looking for paths
 * which satisfy criterion #1 & #2 above. These nodes are then shipped to
 * the major neighbor for that node. By convention, for any node we define
 * its major neighbor to be the neighbor with the largest node id
 * (lexicographically).
 *
 * In the Reducer nodes which could be bubbles are grouped with their major
 * neighbor. For example, nodes A & B would be grouped with node Y in the
 * reducer. If nodes A & B can be merged then the node with lower coverage is
 * deleted and the edges of node Y are updated. The reducer then outputs
 * messages that are used in PopBubblesAvro to tell node X to delete its
 * edges to node B which was deleted.
 *
 * Special Cases:
 * 1. Palindromes. A bubble can be created by a palindrome, a sequence
 *    which equals its reverse complement. Palindromes can be created when
 *    two sequences are merged. This can create a bubble
 *    X->{A, R(A)} -> Y  where A=R(A). Unfortunately, the distributed algorithm
 *    for doing pair wise merges means we can't deal with palindromes when
 *    doing the merge.
 *
 *    The reducer identifies bubbles formed by palindromes. If such a bubble
 *    is detected then the bubble is popped by ensuring all edges from
 *    the major neighbor target the forward strand of the node and all edges
 *    from the minor target the reverse strand of the palindrome.
 *
 * 2. Major and minor node are the same. For example suppose we have the
 *    graph  X->{A, B}->R(X). For the most part we can handle this like
 *    any another bubble. Except that we can pop the bubble in the reducer
 *    since we have access to the minor node.
 *
 * Important: The graph must be maximally be compressed otherwise this code
 * won't work.
 */

public class FindBubblesAvro extends Stage   {
  private static final Logger sLogger = Logger.getLogger(FindBubblesAvro.class);

  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(Schema.create(Schema.Type.STRING),
          (new GraphNodeData()).getSchema());

  public static final Schema REDUCE_OUT_SCHEMA =
      new FindBubblesOutput().getSchema();

  public final static CounterName NUM_BUBBLES =
      new CounterName("Contrail", "find-bubbles-num-bubbles");

  public final static CounterName NUM_PALINDROMES =
      new CounterName("Contrail", "find-bubbles-num-palindromes");

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    // Add options specific to this stage.
    ParameterDefinition bubble_edit_rate =
        new ParameterDefinition("bubble_edit_rate",
            "We consider two sequences to be the same if their edit distance " +
            "is less then or equal to length * bubble_edit_rate.",
            Float.class, new Float(0));

    ParameterDefinition bubble_length_threshold =
        new ParameterDefinition("bubble_length_threshold",
            "A threshold for sequence lengths. Only sequence's with lengths " +
            "less than this value can be bubbles.",
            Integer.class, new Integer(0));

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

  /**
   * The mapper identifies potential bubbles.
   */
  public static class FindBubblesAvroMapper extends
  AvroMapper<GraphNodeData, Pair<CharSequence,GraphNodeData>>    {
    int bubbleLenThresh = 0;
    GraphNode node = null;

    private Pair<CharSequence, GraphNodeData> outPair;

    public void configure(JobConf job)    {
      FindBubblesAvro stage= new FindBubblesAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      bubbleLenThresh =
          (Integer) (definitions.get(
              "bubble_length_threshold").parseJobConf(job));

      node = new GraphNode();
      outPair = new Pair<CharSequence, GraphNodeData>("", new GraphNodeData());
    }

    public void map(GraphNodeData graphData,
        AvroCollector<Pair<CharSequence, GraphNodeData>> output,
        Reporter reporter) throws IOException   {
      node.setData(graphData);

      int sequenceLength = graphData.getSequence().getLength();
      int outDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING);
      int inDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING);
      // check if node can't be a bubble
      if ((sequenceLength >= bubbleLenThresh) || (outDegree != 1) ||
          (inDegree != 1))  {
        outPair.set(node.getNodeId(), graphData);
        output.collect(outPair);
        return;
      }

      // Identify the major neighbor.
      EdgeTerminal in =
          node.getEdgeTerminals(
              DNAStrand.FORWARD, EdgeDirection.INCOMING).get(0);
      EdgeTerminal out =
          node.getEdgeTerminals(
              DNAStrand.FORWARD, EdgeDirection.OUTGOING).get(0);

      CharSequence majorID = GraphUtil.computeMajorID(in.nodeId, out.nodeId);
      outPair.set(majorID, graphData);
      output.collect(outPair);
    }
  }

  /**
   * The reducer.
   */
  public static class FindBubblesAvroReducer
  extends AvroReducer<CharSequence, GraphNodeData, FindBubblesOutput> {
    private int K;
    public float bubbleEditRate;
    private GraphNode majorNode = null;
    private GraphNode middleNode = null;
    private FindBubblesOutput output = null;
    private BubbleUtil bubbleUtil = null;

    public void configure(JobConf job) {
      FindBubblesAvro stage = new FindBubblesAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();

      bubbleEditRate= (Float)(
          definitions.get("bubble_edit_rate").parseJobConf(job));
      K = (Integer)(definitions.get("K").parseJobConf(job));
      if(K <= 0)  {
        throw new RuntimeException("K should be greater than zero.");
      }

      majorNode = new GraphNode();
      middleNode = new GraphNode();
      output = new FindBubblesOutput();
      output.setDeletedNeighbors(new ArrayList<CharSequence>());
      output.setPalindromeNeighbors(new ArrayList<CharSequence>());
      bubbleUtil = new BubbleUtil();
    }


    /**
     * This class describes the Path instance between major and minor node
     * PathBase declares a few abstract functions that are necessary
     * in comparing/popping Paths
     */
    private abstract class PathBase implements Comparable<PathBase>{
      // A boolean variable used for marking nodes which will be deleted because
      // they are sufficiently similar to another node.
      boolean popped;
      // The strands type of the major and minor node 
      abstract DNAStrand getMajorStrand();
      abstract DNAStrand getMinorStrand();
      abstract String getMajorNeighborID();
      abstract Sequence getTrimmedSequence();
      abstract float getCoverage();

      /**
       * compareTo sorts the nodes based on path coverage in descending order.
       */
      public int compareTo(PathBase o) {
        PathBase co = o;
        return (int)(co.getCoverage() - getCoverage());
      }
    }

    /**
     * This class is used by Indirect Paths between major and minor nodes
     * An indirect path is a path between two nodes X &Y which passes through 
     * some middle node e.g X->A->Y is an indirect path between X & Y. 
     * 
     * For this middle Node, we identify the strand such
     * that there is an outgoing edge from the forward strand of its major
     * neighbor. We use this strand when computing the edit distance.
     * 
     * IndirectPath sequence of X->A->Y is interpreted as X + A[K-1:] + Y[K-1:]
     * (where [K-1:] refers to substring from K-1 index to end of sequence)
     * Since we wish to compare sequences of various Indirect Paths; we do not 
     * need to compare X and Y[K-1:] bases of Indirect Path sequence; as they
     * will be same of each path having same majors and minors (as X and Y).
     * 
     * Trimmed Sequence of an Indirect Path returns A[K-1:] sequence for path 
     * X->A->Y (which is enough to compare various Indirect paths)
     */
    class IndirectPath extends PathBase {
      private GraphNode middleNode;
      private String minorID;
      private Boolean isPalindromeValue;
      private DNAStrand middleNodeStrand;
      private DNAStrand majorStrand;
      private DNAStrand minorStrand;
      private Sequence alignedSequence;
      private Sequence trimmedSequence;

      // This boolean indicates we have the special case where the
      // two terminals for the middleNode are different strands of the major
      // node.
      boolean noMinor;

      /**
       * This constructor accepts nodeData one of the middle Node of Indirect 
       * Paths and sets the rest of class instances based on information 
       * contained in nodeData 
       * @param middleNodeData
       * @param major
       */
      public IndirectPath(GraphNodeData middleNodeData, String major, int K) {
        middleNode = new GraphNode();
        middleNode.setData(middleNodeData);
        popped = false;

        Set<String> neighborIds = middleNode.getNeighborIds();

        if (neighborIds.size() > 2) {
          throw new RuntimeException(
              String.format(
                  "Path has more than 2 ends. This is a bug. Number " +
                      "of neighbors is %d.", neighborIds.size()));
        }
        if (neighborIds.size() == 1) {
          // We have the special case where the two terminals for the
          // middle Node are the same node.
          minorID = major;
          noMinor = true;
        } else {
          for (String neighborId : neighborIds) {
            if (!neighborId.equals(major)) {
              minorID = neighborId;
              break;
            }
          }
        }
        // Find the strand of this node which is the terminal for an outgoing
        // edge of the forward strand of the major node.
        // edge from the major node.
        EdgeTerminal terminal =
            middleNode.getEdgeTerminals(
                DNAStrand.FORWARD, EdgeDirection.INCOMING).get(0);
        if (terminal.nodeId.toString().equals(major.toString())) {
          middleNodeStrand = DNAStrand.FORWARD;
        } else {
          middleNodeStrand = DNAStrand.REVERSE;
        }
        alignedSequence = DNAUtil.sequenceToDir(
            middleNode.getSequence(), middleNodeStrand);

        // the sequence of X->A->Y path = A[K-1:] (as noted in class javadoc)
        // here we have A as alignedSequence of middleNode
        trimmedSequence = alignedSequence.subSequence(K-1, alignedSequence.size());

        EdgeTerminal majorTerminal = middleNode.getEdgeTerminals(
            middleNodeStrand, EdgeDirection.INCOMING).get(0);
        EdgeTerminal minorTerminal = middleNode.getEdgeTerminals(
            middleNodeStrand, EdgeDirection.OUTGOING).get(0);
        
        majorStrand = majorTerminal.strand;
        minorStrand = minorTerminal.strand;
      }

      public Boolean isPalindrome() {
        if (isPalindromeValue == null) {
          isPalindromeValue = DNAUtil.isPalindrome(alignedSequence);
        }
        return isPalindromeValue;
      }

      @Override
      float getCoverage() {
        return this.middleNode.getCoverage();
      }

      @Override
      DNAStrand getMajorStrand() {
        return majorStrand;
      }

      @Override
      DNAStrand getMinorStrand() {
        return minorStrand;
      }

      @Override
      Sequence getTrimmedSequence() {
        return trimmedSequence;
      }

      @Override
      String getMajorNeighborID() {
        return middleNode.getNodeId().toString();
      }
    }

    /**
     * This class is used for identifying direct paths that occur between 
     * minor and major nodes
     * 
     * DirectPath sequence of X->Y is interpreted as X + Y[K-1:]
     * (where [K-1:] refers to substring from K-1 index to end of sequence)
     * Since we wish to compare sequences of various Indirect Paths and also 
     * Direct Paths; we do not need to compare X and Y[K-1:] bases of Direct 
     * Path sequence; as they will be same of each path having same majors and
     * minors (as X and Y).
     * 
     * Trimmed Sequence of an Direct Path returns "" (empty) sequence for path 
     * X->Y (which is enough to compare various Indirect paths)
     */
    public class DirectPath extends PathBase{
      private float edgeCoverage;
      private CharSequence minorID;

      private DNAStrand majorStrand;
      private DNAStrand minorStrand;
      private Sequence trimmedSequence;

      public DirectPath(GraphNode majorNode, CharSequence minor, int K)  {

        // Find the strand of major node which has an outgoing edge to the
        // minor node.
        for(DNAStrand strand: DNAStrand.values())  {
          List<EdgeTerminal> terminals = majorNode.getEdgeTerminals(
              strand, EdgeDirection.OUTGOING);
          EdgeTerminal temp = new EdgeTerminal(minor.toString(),
              strand);
          if(terminals.contains(temp)) {
            majorStrand = strand;
            minorStrand = strand;
            break;
          }
        }

        EdgeTerminal minorTerminal = new EdgeTerminal(minor.toString(),
            minorStrand);

        // trimmed sequence is the sequence of middle node minus overlapped
        // bases; Since there is no middle node in direct paths trimmedsequence
        // is an empty sequence 
        trimmedSequence = new Sequence("", DNAAlphabetFactory.create());

        // to calculate number of reads this edge points to; we get tags info.
        // Tag info is never larger than the parameter max thread reads.
        List<CharSequence> tags = majorNode.getTagsForEdge(
            majorStrand, minorTerminal);
        // TODO: when building the graph we should store
        // the actual coverage so that if we don't store all the tags for an edge we still
        // have an accurate representation of the coverage.
        edgeCoverage = tags.size();

        popped = false;
        minorID = minor;
      }

      @Override
      float getCoverage() {
        return edgeCoverage;
      }

      @Override
      DNAStrand getMajorStrand() {
        return majorStrand;
      }

      @Override
      DNAStrand getMinorStrand() {
        return minorStrand;
      }

      @Override
      Sequence getTrimmedSequence() {
        return trimmedSequence;
      }

      @Override
      String getMajorNeighborID() {
        return minorID.toString();
      }
    }

    /**
     *  This function returns all the path from node X to node Y
     *  where X and Y are major and minor nodes (not necessarily in order)
     */
    private void addDirectPathList(CharSequence minor, 
        List<PathBase> minor_list, GraphNode majorNode, int K)  {
      //  By convention in FindBubbles  we align the middle node so that 
      // there is an outgoing edge from the major node to the bubble node. 
      // Either strand of the major node could be involved.
      DirectPath temp = null;
      ArrayList<DirectPath> directPathList = new ArrayList<DirectPath>();
      IndirectPath nodeMetaData = (IndirectPath) minor_list.get(0);
      EdgeTerminal minorTerminal = new EdgeTerminal(minor.toString(),
          nodeMetaData.minorStrand);
      Set<EdgeTerminal> terminals = majorNode.getEdgeTerminalsSet(
          nodeMetaData.majorStrand, EdgeDirection.OUTGOING);
      for(EdgeTerminal terminal: terminals){
        if (terminal.equals(minorTerminal))  {
          temp = new DirectPath(majorNode, minor, K); 
          directPathList.add(temp);
        }
      }
      minor_list.addAll(directPathList);
    }

    /**
     * For every pair of nodes (u, v) we compute the edit distance between the
     * sequences. If the edit distance is less than edit_distance_threshold
     * then the sequences are sufficiently similar and we can mark the path
     * with the smaller coverage to be removed
     */
    protected void markRedundantPaths(
        List<PathBase> processList, Reporter reporter,
        CharSequence minor)  {
      int distance;
      int threshold;

      // Sort potential middle nodes in order of decreasing coverage.
      Collections.sort(processList);      
      for (int i = 0; i < processList.size(); i++)   {
        PathBase highCoveragePath = processList.get(i);
        if(highCoveragePath.popped)  {
          continue;
        }
        for (int j = i+1; j < processList.size(); j++)   {
          PathBase lowCoveragePath = processList.get(j);
          if(lowCoveragePath.popped)  {
            continue;
          }

          // We only want to compare two paths if they are between the same
          // strands of the major and minor node.
          if((highCoveragePath.getMajorStrand() != lowCoveragePath.getMajorStrand()) ||
              (highCoveragePath.getMinorStrand() != lowCoveragePath.getMinorStrand())) {
            continue; 
          }  

          distance = highCoveragePath.getTrimmedSequence().computeEditDistance(
              lowCoveragePath.getTrimmedSequence());

          threshold = (int) Math.max(
              highCoveragePath.getTrimmedSequence().size(),
              lowCoveragePath.getTrimmedSequence().size() * bubbleEditRate);

          reporter.incrCounter("Contrail", "pathschecked", 1);
          if (distance <= threshold)  {

            reporter.incrCounter(NUM_BUBBLES.group, NUM_BUBBLES.tag, 1);
            // Since we compare the trimmed sequence we add 1 to get the length
            // of the sequence that doesn't has overlapped base pairs
            int lowLength =
                lowCoveragePath.getTrimmedSequence().size()+1;

            // Since lowCoveragePath is the path that will be deleted.
            // we need to update coverage of highCoveragePath to reflect the
            // fact that we consider lowCoveragePath to be the same sequence
            // as highCoveragePath except for the read errors.
            float extraCoverage =
                lowCoveragePath.getCoverage() * lowLength;

            lowCoveragePath.popped = true;
            int highLength = highCoveragePath.getTrimmedSequence().size()+1;
            float support = highCoveragePath.getCoverage() * highLength +
                extraCoverage;

            // only set Coverage if middle Node exists
            if(highCoveragePath instanceof IndirectPath)  {
              ((IndirectPath) highCoveragePath).middleNode.setCoverage(
                  support / highLength);
            }
            // remove edges from majorNode to middleNode
            majorNode.removeNeighbor(lowCoveragePath.getMajorNeighborID());
          }
        }
      }
    }

    /**
     * Check every non-popped node to see if its a palindrome. If it is
     * a palindrome then make sure all edges to it are to the forward strand.
     *
     * @param minor_list
     * @param reporter
     */
    void processPalindromes (
        List<PathBase> minorList, Reporter reporter) {
      for (PathBase path: minorList) {
        if(path instanceof IndirectPath) {
          IndirectPath indirectPath = (IndirectPath) path;
          if (indirectPath.popped) {
            continue;
          }
          if (!indirectPath.isPalindrome()) {
            continue;
          }
          reporter.incrCounter(NUM_PALINDROMES.group, NUM_PALINDROMES.tag, 1);

          bubbleUtil.fixEdgesToPalindrome(
              majorNode, indirectPath.middleNode.getNodeId(), true);
          bubbleUtil.fixEdgesFromPalindrome(indirectPath.middleNode);
        }
      }
    }

    /**
     * Output the messages to the minor node. 
     * This function receives the PathBase list and outputs
     * 1. Information to the minor node in the form of following schema: 
     *  -- Minor node ID (to which messages will be sent)
     *  -- List of deleted neighbors to be removed
     *  -- List of neighbors which are palindrome
     *  
     * 2. Nodes from paths which weren't removed of major
     * @param processList
     * @param minor
     * @param collector
     * @throws IOException
     */
    void outputMessages(
        List<PathBase> processList, CharSequence minor,
        AvroCollector<FindBubblesOutput> collector) throws IOException {
      output.setNode(null);
      output.setMinorNodeId("");
      output.getDeletedNeighbors().clear();
      output.getPalindromeNeighbors().clear();

      ArrayList<CharSequence> deletedNeighbors = new ArrayList<CharSequence>();
      ArrayList<CharSequence> palindromeNeighbors =
          new ArrayList<CharSequence>();

      for(PathBase markedPath : processList) {
        if(markedPath instanceof IndirectPath) {
          IndirectPath indirectPath = (IndirectPath) markedPath;

          if(markedPath.popped) {
            deletedNeighbors.add(((IndirectPath) markedPath).middleNode.getNodeId());
          } else {
            if (indirectPath.isPalindrome()) {
              palindromeNeighbors.add(indirectPath.middleNode.getNodeId());
            }
            // This is a non-popped node so output the node.
            output.setNode(indirectPath.middleNode.getData());
            collector.collect(output);    
          }
        } else  {
          DirectPath directPath = (DirectPath) markedPath;
          if(markedPath.popped) {
            deletedNeighbors.add(directPath.minorID);
          }
        }
      }
      if (majorNode.getNodeId().equals(minor.toString())) {
        // For these paths the minor node is the same as the major node
        // so we don't need to send any messages to a minor node.
        return;
      }
      // Output the messages to the minor node.
      output.setNode(null);
      output.setMinorNodeId(minor);
      output.setDeletedNeighbors(deletedNeighbors);
      output.setPalindromeNeighbors(palindromeNeighbors);
      collector.collect(output);
    }

    public void reduce(CharSequence nodeID, Iterable<GraphNodeData> iterable,
        AvroCollector<FindBubblesOutput> collector, Reporter reporter)
            throws IOException {
      // We group IndirectPath based on the minor neighbor for the node.
      Map<String, List<PathBase>> minorToPathsMap =
          new HashMap<String, List<PathBase>>();
      int sawNode = 0;
      Iterator<GraphNodeData> iter = iterable.iterator();

      // We want a string version for use with equals.
      String majorID = nodeID.toString();

      while(iter.hasNext())   {
        GraphNodeData nodeData = iter.next();
        if (majorID.equals(nodeData.getNodeId().toString())) {
          // This node is the major node.
          majorNode.setData(nodeData);
          majorNode = majorNode.clone();
          ++sawNode;
        } else {
          middleNode.setData(nodeData);
          middleNode = middleNode.clone();

          IndirectPath indirectPath = new IndirectPath(
              middleNode.getData(), majorID, K);

          reporter.incrCounter("Contrail", "linkschecked", 1);
          // see if the hashmap has that particular ID of edge terminal;
          // if not then add it
          if (!minorToPathsMap.containsKey(indirectPath.minorID)) {
            List<PathBase> list = new ArrayList<PathBase>();
            minorToPathsMap.put(indirectPath.minorID, list);
          }
          minorToPathsMap.get(indirectPath.minorID.toString()
              ).add(indirectPath);
        }
      }

      if (sawNode == 0)    {
        Formatter formatter = new Formatter(new StringBuilder());
        formatter.format(
            "ERROR: No node was provided for nodeId %s. This can happen if " +
                "the graph isn't maximally compressed before calling FindBubbles", majorID);
        throw new IOException(formatter.toString());
      }

      if (sawNode > 1) {
        Formatter formatter = new Formatter(new StringBuilder());
        formatter.format("ERROR: nodeId %s, %d nodes were provided",
            majorID, sawNode);
        throw new IOException(formatter.toString());
      }

      for (String minorID : minorToPathsMap.keySet())   {
        List<PathBase> minorPaths = minorToPathsMap.get(minorID);
        int choices = minorPaths.size();
        reporter.incrCounter("Contrail", "minorchecked", 1);

        if (choices <= 1) {
          IndirectPath indirectPath = (IndirectPath) minorPaths.get(0);
          // Check if this middleNode is a palindrome.
          if (!DNAUtil.isPalindrome(indirectPath.alignedSequence)) {
            // We have a chain, i.e A->X->B here
            // A->{X,Y,...}->B this shouldn't happen and probably means
            // the graph wasn't maximally compressed.
            throw new RuntimeException(
                "We found a chain. This probably means the " +
                    "graph wasn't maximally compressed before running " +
                "FindBubbles.");
          } else {
            // get add Paths between major and minor
            addDirectPathList(minorID, minorPaths, majorNode,K);
            // marks nodes to be deleted for a particular list of minorID
            markRedundantPaths(minorPaths, reporter, minorID);
          }
          // After removing redundant paths, we check any nodes which are still 
          //alive if they are palindromes.
          processPalindromes(minorPaths, reporter);
          reporter.incrCounter("Contrail", "edgeschecked", choices);
          outputMessages(minorPaths, minorID, collector);
        }

        // Output the major node.
        output.setNode(majorNode.getData());
        output.setMinorNodeId("");
        output.getDeletedNeighbors().clear();
        output.getPalindromeNeighbors().clear();
        collector.collect(output);
      }
    }
  }

    // Run Tool
    ///////////////////////////////////////////////////////////////////////////

    public RunningJob runJob() throws Exception {
      String[] requiredArgs = {"inputpath", "outputpath", "bubble_edit_rate",
      "bubble_length_threshold"};
      checkHasParametersOrDie(requiredArgs);

      String inputPath = (String) stage_options.get("inputpath");
      String outputPath = (String) stage_options.get("outputpath");
      int K = (Integer)stage_options.get("K");

      sLogger.info("Tool name: FindBubbles");
      sLogger.info(" - input: "  + inputPath);
      sLogger.info(" - output: " + outputPath);

      int bubbleLengthThreshold =
          (Integer)stage_options.get("bubble_length_threshold");
      if (bubbleLengthThreshold <= K) {
        sLogger.warn(
            "FindBubbles will not run because bubble_length_threshold<=K so no " +
            "nodes will be considered bubbles.");
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

      long numToPop = result.getCounters().findCounter(
          NUM_BUBBLES.group, NUM_BUBBLES.tag).getValue();

      long numPalindromes = result.getCounters().findCounter(
          NUM_PALINDROMES.group, NUM_PALINDROMES.tag).getValue();

      sLogger.info("Number of nodes to pop:" + numToPop);
      sLogger.info("Number of palindromes:" + numPalindromes);
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