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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.RemoveTipMessage;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.NeighborData;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.stages.GraphCounters.CounterName;

/**
 * removeTips Phase  identifies the 'tips' in the graphdata;
 * These tips are identified by
 * 1. Sum of inDegree and outDegree is at most 1
 * 2. their sequence length being less than a particular limit (tiplength)
 *
 * We can have lots of tips along one strand; and sometimes all the edges in a
 * particular Strand direction are tips, In that case we only keep the longest
 * one and remove all other shorter tips.

 * Mapper:
 *   -- Identify the tips
 *   -- Tell the corresponding neighbor that I am the tip by sending Removetip
 *      Message
 *   -- collect nodeID of terminal and Removetip Message (contains complement of
 *      Strand to neighbor, nodeID of tip)

 * Reducer:
 *  -- we identify the best-tip (longest tip) in both kind of DNAStrands
 *  -- delete rest of the tips for both kind of DNAStrands
 */
public class RemoveTipsAvro extends MRStage {
  private static final Logger sLogger = Logger.getLogger(RemoveTipsAvro.class);

  public static final Schema MAP_OUT_SCHEMA = Pair.getPairSchema(
      Schema.create(Schema.Type.STRING), (new RemoveTipMessage()).getSchema());
  private static Pair<CharSequence, RemoveTipMessage> out_pair =
      new Pair<CharSequence, RemoveTipMessage>(MAP_OUT_SCHEMA);

  public final static CounterName NUM_REMOVED =
      new CounterName("Contrail", "remove-tips-num-clipped");

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition tiplength = new ParameterDefinition(
        "tiplength", "Any sequences longer than this are not considered tips " +
        "or islands and will not be removed. This value should be at least " +
        "K.", Integer.class, null);

    for (ParameterDefinition def: new ParameterDefinition[] {tiplength}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);

    return Collections.unmodifiableMap(defs);
  }

  // RemoveTipsMapper
  ///////////////////////////////////////////////////////////////////////////

  public static class RemoveTipsAvroMapper extends
      AvroMapper<GraphNodeData, Pair<CharSequence, RemoveTipMessage>>  {

    public int tiplength = 0;
    public  GraphNode node= null;
    public static boolean VERBOSE = false;
    public static RemoveTipMessage msg= null;

    @Override
    public void configure(JobConf job) {
      RemoveTipsAvro stage = new RemoveTipsAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      tiplength = (Integer)(definitions.get("tiplength").parseJobConf(job));
      msg= new RemoveTipMessage();
      out_pair = new Pair<CharSequence,  RemoveTipMessage>("", msg);
    }

    @Override
    public void map(GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, RemoveTipMessage>> output,
        Reporter reporter) throws IOException  {
      node = new GraphNode(graph_data);
      int fdegree = node.degree(DNAStrand.FORWARD);
      int rdegree = node.degree(DNAStrand.REVERSE);

      int inDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING);
      int outDegree = node.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING);

      int len     = graph_data.getSequence().getLength();

      if ((inDegree == 0) && (outDegree == 0))   {
        if (node.getSequence().size() <= tiplength) {
          reporter.incrCounter("Contrail", "RemoveTips-island-removed", 1);
          return;
        }

        msg.setNode(graph_data);
        msg.setEdgeStrands(null);
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
        return;
      }

      // We need to make sure the node doesn't have connected strands.
      // e.g A->R(A) inDegree + outdegree = 1 because the edge is only
      // stored once.
      // a cycle A->A would have indgree + outdegree =2.
      if ((len <= tiplength) && (inDegree + outDegree <= 1) &&
          !node.hasConnectedStrands())  {
        reporter.incrCounter("Contrail", "tips_found", 1);

        if (VERBOSE)	{
          sLogger.info("Removing tip " + node.getNodeId() + " len=" + len);
        }

        // Tell the one neighbor that I'm a tip
        DNAStrand strand;
        if (fdegree == 1) {
          strand = DNAStrand.FORWARD;
        }
        else {
          strand = DNAStrand.REVERSE;
        }
        List<EdgeTerminal> terminals = node.getEdgeTerminals(strand, EdgeDirection.OUTGOING);
        StrandsForEdge key = StrandsUtil.form(strand, terminals.get(0).strand);

        msg.setNode(graph_data);
        msg.setEdgeStrands(key);
        out_pair.set( terminals.get(0).nodeId, msg);
        output.collect(out_pair);
      } else	{
        msg.setNode(graph_data);
        msg.setEdgeStrands(null); /*setEdgeStrands is set null to indicate
							  that this node is normal, not a tip*/
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);
        reporter.incrCounter("Contrail", "nodes", 1);
      }
    }
  }

  // RemoveTipsReducer
  ///////////////////////////////////////////////////////////////////////

  public static class RemoveTipsAvroReducer
  extends AvroReducer<CharSequence, RemoveTipMessage, GraphNodeData>   {
    GraphNode temp_node = null;
    GraphNode actual_node= null;
    GraphNode tip_node = null;

    @Override
    public void configure(JobConf job) {
      temp_node = new GraphNode();
      actual_node= new GraphNode();
      tip_node= new GraphNode();
    }

    // identifies the best-tip (longest tip) for a particular kind of DNAStrands
    int LongestTip(List<RemoveTipMessage> msg_list)	{
      int bestlen = 0;
      RemoveTipMessage besttip_msg = null;

      for (RemoveTipMessage message : msg_list)	{
        tip_node.setData(message.getNode());
        int len = tip_node.getData().getSequence().getLength();
        if (len > bestlen)	{
          bestlen = len;
          besttip_msg = message;
        }
      }
      return besttip_msg.getNode().getSequence().getLength();
    }

    @Override
    public void reduce(CharSequence nodeid, Iterable<RemoveTipMessage> iterable,
        AvroCollector<GraphNodeData> output, Reporter reporter)
            throws IOException   {

      Iterator<RemoveTipMessage> iter = iterable.iterator();

      //-- set-up 2 lists in a HashMap keyed by DNAStrand and its corresponding message from mapper
      //-- Thus HashMap has 2 entries as lists; one list corresponds to Forward EdgeStrands generated from mapper; and other corresponds to Reverse Edgestrands
      //-- populate the 2 lists using the output from the mapper sent to reducer for a particular terminal (whose nodeID is sent as key);

      Map<DNAStrand, List<RemoveTipMessage>> tips = new HashMap<DNAStrand, List<RemoveTipMessage>>();

      List<RemoveTipMessage> f_msglist = new ArrayList<RemoveTipMessage>();
      tips.put(DNAStrand.FORWARD, f_msglist);
      List<RemoveTipMessage> r_msglist = new ArrayList<RemoveTipMessage>();
      tips.put(DNAStrand.REVERSE, r_msglist);

      int sawnode = 0;

      while(iter.hasNext())	{
        RemoveTipMessage msg = iter.next();
        if (msg.getEdgeStrands() == null)    {  // non tip , normal node
          actual_node.setData(msg.getNode());
          actual_node = actual_node.clone();

          sawnode++;
        }
        else 	{
          RemoveTipMessage copy = new RemoveTipMessage();
          copy.setEdgeStrands(msg.getEdgeStrands());
          temp_node.setData(msg.getNode());
          temp_node = temp_node.clone();
          copy.setNode(temp_node.getData());
          DNAStrand dnastrand= StrandsUtil.dest(copy.getEdgeStrands() );
          tips.get(dnastrand).add(copy);
        }
      }

      if (sawnode != 1)	{
        throw new IOException("ERROR: Didn't see exactly 1 NON-tip node (" + sawnode + ") for " + nodeid.toString());
      }
      for(DNAStrand strand: DNAStrand.values())	{
        int deg = 0;
        int numTips = 0;
        int besttip_len = 0;
        NeighborData result = null;
        boolean keptTip = false;

        List<RemoveTipMessage> msg_list = tips.get(strand);
        numTips += msg_list.size();
        if (numTips == 0) { continue; }
        deg = actual_node.degree(strand, EdgeDirection.INCOMING);
        if (numTips == deg)	{
          // All edges in this direction are tips, only keep the longest one
          besttip_len = LongestTip(msg_list);
        }
        /* if the number of tips is > 0 but not equal to the degree
	         of the non tip node;then we'll remove all the tips and
	         leave non-tips intact
	         the tips with same length are removed
	       */
        for (RemoveTipMessage message : msg_list)   {
          tip_node.setData(message.getNode());
          if(numTips == deg)	{
            if( tip_node.getData().getSequence().getLength() < besttip_len )    { // check if its len < len of longest tip
              result = actual_node.removeNeighbor(tip_node.getNodeId());
            }
            // in case of a tie; we keep only one of the tips
            else  if(tip_node.getData().getSequence().getLength() == besttip_len) {
              if(!keptTip)  {
                output.collect(tip_node.getData());
                keptTip=true;
                reporter.incrCounter("Contrail", "tips_kept", 1);
              }
              else  {
                result = actual_node.removeNeighbor(tip_node.getNodeId());
              }
            }
          }
          else	{
            // remove all
            result = actual_node.removeNeighbor(tip_node.getNodeId());
          }

          if(result != null)    {
            reporter.incrCounter(NUM_REMOVED.group, NUM_REMOVED.tag, 1);
          }
        }
      }
      output.collect(actual_node.getData());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf)getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, RemoveTipsAvro.MAP_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, RemoveTipsAvroMapper.class);
    AvroJob.setReducerClass(conf, RemoveTipsAvroReducer.class);
    AvroJob.setOutputSchema(conf, graph_data.getSchema());
  }

  @Override
  protected void postRunHook() {
    try {
      long numTips = job.getCounters().findCounter(
          NUM_REMOVED.group, NUM_REMOVED.tag).getValue();
      long numNodes = job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "REDUCE_OUTPUT_RECORDS").getValue();
      sLogger.info("Number of tips removed:" + numTips);
      sLogger.info("Number of nodes outputted:" + numNodes);
    } catch (IOException e) {
      sLogger.fatal("Couldn't get counters.", e);
      System.exit(-1);
    }
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> items = super.validateParameters();

    int tiplength =  (Integer) stage_options.get("tiplength");
    int K = (Integer)stage_options.get("K");
    if (tiplength <= K) {
      InvalidParameter item = new InvalidParameter(
          "tiplength",
          "tiplength can't be less than K because no nodes would be removed.");
      items.add(item);
    }
    return items;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new RemoveTipsAvro(), args);
    System.exit(res);
  }
}
