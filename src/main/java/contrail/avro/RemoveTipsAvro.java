package contrail.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.ContrailConfig;
import contrail.CompressedSequence;
import contrail.GraphNodeData;
import contrail.RemoveTipMessage;
import contrail.Stats;
import contrail.sequences.DNAUtil;


public class RemoveTipsAvro extends Stage
{	
  private static final Logger sLogger = Logger.getLogger(RemoveTipsAvro.class);

  public static final Schema MAP_OUT_SCHEMA = 
      Pair.getPairSchema(Schema.create(Schema.Type.STRING), 
                         (new RemoveTipMessage()).getSchema());
  
  // RemoveTipsMapper
  ///////////////////////////////////////////////////////////////////////////

  private static class RemoveTipsMapper extends 
  AvroMapper<GraphNodeData, 
  Pair<String, RemoveTipMessage>>     
  {
    private static int K = 0;
    public static int TIPLENGTH = 0;
    public static boolean VERBOSE = false;

    public void configure(JobConf job) {

      // TODO(jlewi): Once we update BuildGraph to output 
      // CompressedSequence for the keys then we no longer need to pass in
      // K because K will be stored with each pair.
      K = Integer.parseInt(job.get("K"));
      TIPLENGTH = Integer.parseInt(job.get("TIPLENGTH"));
    }

    @Override
    public void map(GraphNodeData input,
        AvroCollector<Pair<String, RemoveTipMessage>> output, 
        Reporter reporter) throws IOException { 
      //			Node node = new Node();
      //			node.fromNodeMsg(nodetxt.toString());
      //
      GraphNode node = new GraphNode();
      node.setData(input);

      int fdegree = node.degree("f");
      int rdegree = node.degree("r");
      int len     = input.getCanonicalSourceKmer().getLength();
      //
      if ((len <= TIPLENGTH) && (fdegree + rdegree <= 1)) {
        // Since the total degree for this node is <=1, this node
        // is the tip of a sequence.
        reporter.incrCounter("Contrail", "tips_found", 1);

        if (VERBOSE) {
          System.err.println("Removing tip " + node.getNodeId() + 
              " len=" + len);
        }

        if ((fdegree == 0) && (rdegree == 0)) {
          // This node is not connected to the rest of the graph so remove
          // this node by not outputting it.

          reporter.incrCounter("Contrail", "tips_island", 1);
        }
        else {
          // Tell the one neighbor that I'm a tip
          String linkdir = (fdegree == 0) ? "r" : "f";

          for(String adj : GraphNode.DIRS) {
            String key = linkdir + adj;

            List<CompressedSequence> edges = 
                node.getCanonicalDestForLinkDir(key);

            if (edges != null) {
              if (edges.size() != 1) {
                throw new IOException("Expected a single edge from " + 
                    node.getNodeId());
              }

              // Get the destination sequence
              // String p = edges.get(0);
              CompressedSequence canonical_dest = edges.get(0); 

              if (canonical_dest.equals(
                  node.getData().getCanonicalSourceKmer())) {
                // Short tandem repeat, trim away. Since the destination
                // sequence is the same as the source sequence they are both
                // represented by this node. So we can just drop this 
                // node in order to remove it from the graph.
                reporter.incrCounter("Contrail", "tips_shorttandem", 1);
              }
              else {
                // We can identify the incoming edges to this node by
                // looking at the outgoing edges for the reverse complement
                // of this sequence which are also stored in this node.
                // We send a message to those nodes to delete this node 
                // from their list of outgoing edges.
                String con = DNAUtil.flip_dir(adj) + 
                             DNAUtil.flip_dir(linkdir.toString());
                Pair<String, RemoveTipMessage> pair = new 
                    Pair<String, RemoveTipMessage>(MAP_OUT_SCHEMA);

                RemoveTipMessage message = new RemoveTipMessage();
                message.setLinkDir(con);
                // TODO(jlewi): message shouldn't store CanonicalDestKMer
                // instead it should store the id for the node.
                //message.setCanonicalDestKMer(
                //    node.getData().getCanonicalSourceKmer());

                // TODO(jlewi): We need to set the node_id properly 
                // or else the code won't work.
                String node_id = "";
                pair.set(node_id, message);

                output.collect(pair);
              }
            }
          }
        }
      }
      else {
        // TODO(jlewi): Code is a mess and needs to be updated
        throw new IOException("Code not updated yet");
        // TODO(jlewi): As soon as we update BuildGraphAvro to output
        // a CompressedSequence for the key we can avoid creating 
        // src_seq here and just output the input.
        //CompressedSequence src_seq = new CompressedSequence();
//        src_seq.setDna(input.key());
//        src_seq.setLength(this.K);
//        // Output this node so that we store the graph.
//        Pair<CompressedSequence, RemoveTipMessage> pair = new 
//            Pair<CompressedSequence, RemoveTipMessage>(src_seq, input.value());			  
//        output.collect(pair);
      }

      reporter.incrCounter("Contrail", "nodes", 1);
    }
  }

  // RemoveTipsReducer
  ///////////////////////////////////////////////////////////////////////////

  private static class RemoveTipsReducer extends  
  AvroReducer<String, RemoveTipMessage, 
  GraphNodeData> 
  {
    private static int K = 0;

    public void configure(JobConf job) {
      K = Integer.parseInt(job.get("K"));
    }

    @Override
    public void reduce(
        String key, Iterable<RemoveTipMessage> iterable,
        AvroCollector< GraphNodeData> collector,
        Reporter reporter)
            throws IOException 
            {
            
      throw new IOException("Code not updated yet");
//      GraphNode node = new GraphNode();
//      //node.setData(input.value());
//      //			Node node = new Node(nodeid.toString());
//
//      // tips is a list of the tips for this node. The keys 
//      // are the canonical dest kmers. The values are a list of strings
//      // representing the two letter codes for which this sequence is a tip.
//      Map<CompressedSequence, HashSet<String>> tips = 
//          new HashMap<CompressedSequence, List<String>>();
//      //		
//      // Sawnode keeps track of how many times we saw an actual GraphNodeData
//      // in RemoveTipeMessage.
//      int sawnode = 0;
//
//      for (Iterator<RemoveTipMessage> iter = iterable.iterator(); 
//          it.hasNext(); ) {
//        //			while(iter.hasNext())
//        //			{
//        //				String msg = iter.next().toString();
//        //				
//        //				//System.err.println(key.toString() + "\t" + msg);
//        //				
//        //				String [] vals = msg.split("\t");
//        //				
//        RemoveTipMessage msg = iter.next();
//        if (msg.getGraphNodeData() != null) {
//          // Do We need to make a copy of GraphNodeData? My guess is 
//          // that the iterator is reusing the same objects.
//          node.setData(new GraphNodeData(msg.getGraphNodeData()));
//          sawnode++;
//        }
//        else if (msg.getCanonicalDestKMer() != null) {
//          if (msg.getLinkDir == null) {
//            throw new IOException("link_dir was null in the message.");
//          }
//
//          HashSet<String> link_dirs = tips.get(msg.getCanonicalDestKMer());
//          if (link_dirs == null) {
//            link_dirs = new HashSet<String>();
//            tips.put(msg.getCanonicalDestKMer(), link_dirs);
//          }
//
//          link_dirs.add(msg.getLinkDir());
//          //					String adj = vals[1];
//          //					
//          //					Node tip = new Node(vals[2]);
//          //					tip.parseNodeMsg(vals, 3);
//          //					
//          //					if (tips.containsKey(adj))
//          //					{
//          //						tips.get(adj).add(tip);
//          //					}
//          //					else
//          //					{
//          //						List<Node> tiplist = new ArrayList<Node>();
//          //						tiplist.add(tip);
//          //						tips.put(adj, tiplist);
//          //					}
//          //				}
//          //				else
//          //				{
//          //					throw new IOException("Unknown msgtype: " + msg);
//          //				}
//        }
//        //			
//        if (sawnode != 1) {
//          throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + 
//              sawnode + ") for " + nodeid.toString());
//        }
//        throw new RuntimeException("1-30-2011 need to finish updating the " + 
//            "code below."); 
//        //			
//        //			if (tips.size() != 0)
//        //			{
//        //				for(String d : Node.dirs)
//        //				{
//        //					int deg = node.degree(d);
//        //					
//        //					int numtrim = 0;
//        //					
//        //					List<Node> ftips = tips.get(d+"f");
//        //					List<Node> rtips = tips.get(d+"r");
//        //					
//        //					if (ftips != null) { numtrim += ftips.size(); }
//        //					if (rtips != null) { numtrim += rtips.size(); }
//        //					
//        //					if (numtrim == 0) { continue; }
//        //					
//        //					Node besttip = null;
//        //					int bestlen = 0;
//        //					
//        //					if (numtrim == deg)
//        //					{
//        //						// All edges in this direction are tips, only keep the longest one
//        //						
//        //						if (ftips != null)
//        //						{
//        //							for (Node t : ftips)
//        //							{
//        //								if (t.len() > bestlen)
//        //								{
//        //									bestlen = t.len();
//        //									besttip = t;
//        //								}
//        //							}
//        //						}
//        //						
//        //						if (rtips != null)
//        //						{
//        //							for (Node t : rtips)
//        //							{
//        //								if (t.len() > bestlen)
//        //								{
//        //									bestlen = t.len();
//        //									besttip = t;
//        //								}
//        //							}
//        //						}
//        //						
//        //						output.collect(new Text(besttip.getNodeId()), new Text(besttip.toNodeMsg()));
//        //						reporter.incrCounter("Contrail", "tips_kept", 1);
//        //					}
//        //					
//        //					
//        //					if (ftips != null)
//        //					{
//        //						String adj = d+"f";
//        //						
//        //						for (Node t : ftips)
//        //						{
//        //							if (t != besttip)
//        //							{
//        //								node.removelink(t.getNodeId(), adj);
//        //								reporter.incrCounter("Contrail", "tips_clipped", 1);
//        //							}
//        //						}
//        //					}
//        //					
//        //					if (rtips != null)
//        //					{
//        //						String adj = d+"r";
//        //						
//        //						for (Node t : rtips)
//        //						{
//        //							if (t != besttip)
//        //							{
//        //								node.removelink(t.getNodeId(), adj);
//        //								reporter.incrCounter("Contrail", "tips_clipped", 1);
//        //							}
//        //						}
//        //					}
//        //				}
//        //			}
//        //			
//        //			output.collect(nodeid, new Text(node.toNodeMsg()));
//      }

    } // end reduce
  }

  // Run
  ///////////////////////////////////////////////////////////////////////////

  public RunningJob run(String inputPath, String outputPath) throws Exception
  { 
    throw new NotImplementedException();
//    sLogger.info("Tool name: RemoveTips");
//    sLogger.info(" - input: "  + inputPath);
//    sLogger.info(" - output: " + outputPath);
//
//    JobConf conf = new JobConf(Stats.class);
//    conf.setJobName("RemoveTips " + inputPath + " " + ContrailConfig.K);
//    ContrailConfig.initializeConfiguration(conf);
//
//    FileInputFormat.addInputPath(conf, new Path(inputPath));
//    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//    conf.setInputFormat(TextInputFormat.class);
//    conf.setOutputFormat(TextOutputFormat.class);
//
//    conf.setMapOutputKeyClass(Text.class);
//    conf.setMapOutputValueClass(Text.class);
//
//    conf.setOutputKeyClass(Text.class);
//    conf.setOutputValueClass(Text.class);
//
//    conf.setMapperClass(RemoveTipsMapper.class);
//    conf.setReducerClass(RemoveTipsReducer.class);
//
//    //delete the output directory if it exists already
//    FileSystem.get(conf).delete(new Path(outputPath), true);
//
//    return JobClient.runJob(conf);
  }


  // Parse Arguments and Run
  ///////////////////////////////////////////////////////////////////////////

  @Override
  protected int run() throws Exception 
  {
    throw new NotImplementedException();
//    String inputPath  = "/Users/mschatz/try/02-initialcmp";
//    String outputPath = "/users/mschatz/try/03-removetips.1";
//    ContrailConfig.K = 21;
//    run(inputPath, outputPath);
//    return 0;
  }

  // Main
  ///////////////////////////////////////////////////////////////////////////

  public static void main(String[] args) throws Exception 
  {
    int res = ToolRunner.run(new Configuration(), new RemoveTipsAvro(), args);
    System.exit(res);
  }
}
