package contrail.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.avro.mapred.AvroCollector;
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

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;


public class PairMarkAvro extends Stage {	
	private static final Logger sLogger = Logger.getLogger(PairMarkAvro.class);
	
	private static class CompressibleNode {
	  private CompressibleNodeData data;
	  private GraphNode node;
	  
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
	
	private static class PairMarkMapper extends 
	  AvroMapper<CompressibleNodeData, Pair<CharSequence, PairMarkOutput>> {
		private static long randseed = 0;
		private Random rfactory = new Random();
		
		public void configure(JobConf job) {
			randseed = Long.parseLong(job.get("randseed"));
		}
		
		private enum CoinFlip {
		  Heads, Tails;
		}
		
		// Flip a coin for the given node. The seed for the random 
		// generator is a combination of a global seed and the nodeid.
		// Thus, all nodes compute the same value for the flip for a given node.
		public CoinFlip flip(String nodeid) {
			rfactory.setSeed(nodeid.hashCode() ^ randseed);
			
			double rand = rfactory.nextDouble();			
			CoinFlip side = (rand >= .5) ? CoinFlip.Heads : CoinFlip.Tails;
			
			return side;
		}
		
		
		public TailData getBuddy(CompressibleNode node, DNAStrand strand) {
			if (node.canCompress(strand)) {
				return node.getNode().getTail(strand, EdgeDirection.OUTGOING);
			}			
			return null;
		}
		
		private CompressibleNode node;
		private PairMarkOutput pair_mark_output;
		
		// Output pair
		private Pair<CharSequence, PairMarkOutput> out_pair;
		
		private void clearPairMarkOutput (output) {
		  output.setMessage(null);
		  output.setNode(null);
		}
		public void map(CompressibleNodeData node_data, 
		    AvroCollector<Pair<CharSequence, PairMarkOutput>> output, 
        Reporter reporter) throws IOException {
			
		
		  node.setData(node_data);
		  
		  // Check if either the forward or reverse strand can be merged.
			TailData fbuddy = getBuddy(node, DNAStrand.FORWARD);
			TailData rbuddy = getBuddy(node, DNAStrand.REVERSE);

			// Output the node
			claerPairMarkOutput(pair_mark_output);
			output.setNode(node.getNode().getData());
			out_pair.key(node_data.getNode().getNodeId());
			out_pair.value(pair_mark_output);
			
						
			output.collect(out_pair);
      reporter.incrCounter("Contrail", "nodes", 1);
      
      if (fbuddy == null && rbuddy == null) {
        // Node can't be compressed so we are done.
        return;
      }
      
				reporter.incrCounter("Contrail", "compressible", 1);

				// The terminal for the edge we are compressing.
				EdgeTerminal compress_terminal = null;
				
				// Which strand of this node gets compressed.
				DNAStrand compress_strand = null;
//				String compress = null;
//				String compressdir = null;
//				String compressbdir = null;

				if (flip(node.getNode().getNodeId()) == CoinFlip.Heads) {
				  // This node is a heads.
				  
					// Prefer Merging forward if we can.
				  // We can only merge in a single direction at a time.
					if (fbuddy != null) {
						
					  CoinFlip f_flip = flip(fbuddy.terminal.nodeId);

						if (f_flip == CoinFlip.Tails) {
						  // We can compress the forward strand.
							compress_terminal = fbuddy.terminal;
							compress_strand = DNAStrand.FORWARD;							
						}
					}

					// If we can't compress the forward strand, see if
					// we can compress the reverse strand.
					if ((compress_terminal == null) && (rbuddy != null))
					{
						CoinFlip r_flip = flip(rbuddy.terminal.nodeId);

						if (r_flip == CoinFlip.Tails) {
							compress_terminal = rbuddy.terminal;
							compress_strand  = DNAStrand.REVERSE;							
						}
					}
				} else {
				  LEFT OF WRITING CODE HERE
					if ((rbuddy != null) && (fbuddy != null))
					{
						boolean fmale = isMale(fbuddy.id);
						boolean rmale = isMale(rbuddy.id);

						if (!fmale && !rmale &&
								(nodeid.compareTo(fbuddy.id) < 0) && 
								(nodeid.compareTo(rbuddy.id) < 0))
						{
							// FFF and I'm the local minimum, go ahead and compress
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}
					else if (rbuddy == null)
					{
						boolean fmale = isMale(fbuddy.id);

						if (!fmale && (nodeid.compareTo(fbuddy.id) < 0))
						{
							// Its X*=>FF and I'm the local minimum
							compress     = fbuddy.id;
							compressdir  = "f";
							compressbdir = fbuddy.dir;
						}
					}
					else if (fbuddy == null)
					{
						boolean rmale = isMale(rbuddy.id);

						if (!rmale && (nodeid.compareTo(rbuddy.id) < 0))
						{
							// Its FF=>X* and I'm the local minimum
							compress     = rbuddy.id;
							compressdir  = "r";
							compressbdir = rbuddy.dir;
						}
					}
				}

				if (compress != null)
				{
					//print STDERR "compress $nodeid $compress $compressdir $compressbdir\n";
					reporter.incrCounter("Contrail","mergestomake", 1);

					//Save that I'm supposed to merge
					node.setMerge(compressdir);

					// Now tell my ~CD neighbors about my new nodeid
					String toupdate = Node.flip_dir(compressdir);

					for(String adj : Node.dirs)
					{
						String key = toupdate + adj;

						String origadj = Node.flip_dir(adj) + compressdir;
						String newadj  = Node.flip_dir(adj) + compressbdir;

						List<String> edges = node.getEdges(key);

						if (edges != null)
						{
							for (String p : edges)
							{
								reporter.incrCounter("Contrail", "remoteupdate", 1);
								output.collect(new Text(p), 
										       new Text(Node.UPDATEMSG + "\t" + nodeid + "\t" + origadj + "\t" + compress + "\t" + newadj));
							}
						}
					}
				}				
    }
	}
	
	private static class PairMarkReducer extends MapReduceBase 
	implements Reducer<Text, Text, Text, Text> 
	{
		private static long randseed = 0;
		
		public void configure(JobConf job) {
			randseed = Long.parseLong(job.get("randseed"));
		}
		
		private class Update
		{
			public String oid;
			public String odir;
			public String nid;
			public String ndir;
		}
		
		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException 
		{
			Node node = new Node(nodeid.toString());
			List<Update> updates = new ArrayList<Update>();
			
			int sawnode = 0;
			
			while(iter.hasNext())
			{
				String msg = iter.next().toString();
				
				//System.err.println(key.toString() + "\t" + msg);
				
				String [] vals = msg.split("\t");
				
				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
				else if (vals[0].equals(Node.UPDATEMSG))
				{
					Update up = new Update();
					
					up.oid  = vals[1];
					up.odir = vals[2];
					up.nid  = vals[3];
					up.ndir = vals[4];
					
					updates.add(up);
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}
			}
			
			if (sawnode != 1)
			{
				throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}
			
			if (updates.size() > 0)
			{
				for(Update up : updates)
				{
					node.replacelink(up.oid, up.odir, up.nid, up.ndir);
				}
			}
			
			output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}

	
	public RunningJob run(String inputPath, String outputPath, long randseed) throws Exception
	{ 
		sLogger.info("Tool name: PairMark");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		sLogger.info(" - randseed: " + randseed);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("PairMark " + inputPath);
		
		ContrailConfig.initializeConfiguration(conf);
		conf.setLong("randseed", randseed);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PairMarkMapper.class);
		conf.setReducerClass(PairMarkReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	
	
	public int run(String[] args) throws Exception 
	{
		String inputPath  = "/Users/mschatz/try/compressible/";
		String outputPath = "/users/mschatz/try/mark1/";
		long randseed = 123456789;
		
		run(inputPath, outputPath, randseed);
		
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new PairMark(), args);
		System.exit(res);
	}


  @Override
  protected int run() throws Exception {
    // TODO Auto-generated method stub
    return 0;
  }
}
