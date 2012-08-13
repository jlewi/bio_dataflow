/**
 *
 */
package contrail.stages;
import java.io.IOException;
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

import contrail.QuickMarkMessage;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.KMerReadTag;
import contrail.sequences.StrandsForEdge;

/**
 * @author avijit
 * QuickMark
   We use QuickMark + QuickMerge when the number of compressible nodes is
   below some threshold. In this case, we can send all the nodes to be
   compressed to a single reducer so that we can compress all the chains in
   one shot.

   Since the graph is represented as a collection of CompressibleNodeData,
   we already know which nodes in the graph are compressible.

   However, we also need to know which nodes are connected to nodes which
   will be compressed. e.g suppose we have the graph
   A->B->C->D
   E->B->C->D

   So the nodes B,C,D will all be marked as compressible. However, after
   compressing B,C,D we need to update the edges from A and E. So we need
   the nodes A and E to be marked such that they get sent to the same
   reducer as B,C,D when doing the merge.

   The point of QuickMark is thus to set the "mertag" such that  when
   QuickMerge is run A,B,C,D,E all get same to the same reducer and we can
   do the merge in one shot.

 * Mapper:  the mapper takes in CompressibleNodeData and sees if any node has strands that need to be compressed
 *          if there are NO Compressible Strands for that node
 *		mapper outputs nodeID as key and its CompressibleStrands as msg
 *	    if there are Compressible strands for that node that means compression needs to be done
 *		in that case mapper outputs the neighbor id as key; message object is passed as null (to differentiate)
 * Reducer: the input to reducer is the Iterator of the message
 *          if there were no Compressible Strands for the key; i.e. if its value (message) is not null
 *		then it sets MerTag as the hash-code of the nodeID
 *	    if there are Compressible strands (message is null)
 *		then it sets MerTag as 0 for that node
 */
public class QuickMarkAvro extends Stage     {
  private static final Logger sLogger = Logger.getLogger(QuickMarkAvro.class);
  public static final Schema REDUCE_OUT_SCHEMA =
      new GraphNodeData().getSchema();


  public static class QuickMarkMapper extends
  AvroMapper<CompressibleNodeData, Pair<CharSequence, QuickMarkMessage>> {
    public QuickMarkMessage msg= null;
    public GraphNode node= null;

    private Pair<CharSequence, QuickMarkMessage> out_pair;
    public void configure(JobConf job) {
      node = new GraphNode();
      msg= new QuickMarkMessage();
      out_pair = new Pair<CharSequence,  QuickMarkMessage>("", msg);
    }

    public void map(CompressibleNodeData compressible_graph_data,
        AvroCollector<Pair<CharSequence, QuickMarkMessage>> output,
        Reporter reporter) throws IOException {

      node.setData(compressible_graph_data.getNode());
      CompressibleStrands node_compression_dir = compressible_graph_data.getCompressibleStrands();

      if (node_compression_dir!= CompressibleStrands.NONE)	   {

        // tell all of my neighbors I intend to compress
        reporter.incrCounter("Contrail", "compressible", 1);
        for(StrandsForEdge strand: StrandsForEdge.values())	{
          List<CharSequence> edges = node.getNeighborsForStrands(strand);
          if(edges!=null)		{
            for(CharSequence nodeId: edges)	   {
              msg.setNode(null);
              msg.setSendToCompressor(true);
              out_pair.set(nodeId, msg); // A null node value in msg is used in the following stage
              output.collect(out_pair);
            }
          }
        }
        msg.setNode(node.getData());
        msg.setSendToCompressor(true);
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);

      }
      else	{
        // if compressible strands== NONE; then we only need to send this node to the compressor in the reducer if it recieves messages in the reducer with send_to_compressor = true indicating its neighbors will be compressed
        msg.setNode(node.getData());
        msg.setSendToCompressor(false);
        out_pair.set(node.getNodeId(), msg);
        output.collect(out_pair);
      }
      reporter.incrCounter("Contrail", "nodes", 1);
    }
  }

  public static class QuickMarkReducer extends
  AvroReducer<CharSequence, QuickMarkMessage, GraphNodeData> {

    GraphNode node = null;

    public void configure(JobConf job) {
      node= new GraphNode();
    }

    @Override
    public void reduce(CharSequence  nodeid, Iterable<QuickMarkMessage> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter) throws IOException  {
      boolean compresspair = false;

      Iterator<QuickMarkMessage> iter = iterable.iterator();
      int sawnode = 0;

      while(iter.hasNext())	{
        QuickMarkMessage msg = iter.next();

        if(msg.getNode() != null)   {
          node.setData(msg.getNode());
          node= node.clone();
          sawnode++;
        }
        if (msg.getSendToCompressor() == true)	{
          // This must be a message of compression i.e. whose CompressibleStrands were not NONE
          compresspair = true;
        }
      }

      if (sawnode != 1)	{
        throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
      }
      if (compresspair)     {
        KMerReadTag readtag = new KMerReadTag("compress", 0);
        //when QuickMerge is run all nodes that need to be compressed or are connected to compressed nodes will be sent to the same reducer
        node.setMertag(readtag);
        reporter.incrCounter(
            GraphCounters.quick_mark_nodes_send_to_compressor.group,
            GraphCounters.quick_mark_nodes_send_to_compressor.tag, 1);
      }
      else	{
        KMerReadTag readtag = new KMerReadTag(node.getNodeId(), node.getNodeId().hashCode());
        node.setMertag(readtag);
      }
      collector.collect(node.getData());
    }
  }

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

  @Override
  public RunningJob runJob() throws Exception {
    // TODO: set stage options using new method
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    Pair<CharSequence, QuickMarkMessage> map_output =
        new Pair<CharSequence, QuickMarkMessage>("", new QuickMarkMessage());

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    AvroJob.setInputSchema(conf, compressible_node.getSchema());

    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
    AvroJob.setOutputSchema(conf, QuickMarkAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, QuickMarkMapper.class);
    AvroJob.setReducerClass(conf, QuickMarkReducer.class);

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
    int res = ToolRunner.run(new Configuration(), new QuickMarkAvro(), args);
    System.exit(res);
  }
}
