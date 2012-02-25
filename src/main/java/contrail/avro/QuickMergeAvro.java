package contrail.avro;

import contrail.sequences.CompressedSequence;
import contrail.ContrailConfig;

import contrail.Stats;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

import java.io.IOException;
import java.nio.ByteBuffer;
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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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

public class QuickMergeAvro extends Stage 
{	
	private static final Logger sLogger = Logger.getLogger(QuickMergeAvro.class);

	/**
   * Define the schema for the mapper output. The keys will be a string
   * containing the id for the node. The value will be an instance of  
   * GraphNodeData.
   */
  public static final Schema MAP_OUT_SCHEMA = 
      Pair.getPairSchema(Schema.create(Schema.Type.STRING), 
                         (new GraphNodeData()).getSchema());
  
	/**
   * Define the schema for the reducer output. The keys will be a byte buffer
   * representing the compressed source KMer sequence. The value will be an 
   * instance of GraphNodeData. 
   */
  public static final Schema REDUCE_OUT_SCHEMA = 
      new GraphNodeData().getSchema();
  
	/**
	 * Subclass of GraphNode which adds fields we need to track during QuickMerge.
	 */
	private static class GraphNodeForQuickMerge extends GraphNode {
	  /**
	   * Keeps track of whether we have processed this node.
	   */
	  public boolean done = false;
	  
	  /**
	   * Whether we can delete this node or not.
	   */
	  public boolean deletable = false;
	}
	private static class QuickMergeMapper extends 
	  AvroMapper<GraphNodeData, Pair<String, GraphNodeData>> {
		private static GraphNode node = new GraphNode();

		private static Pair<String, GraphNodeData> out_pair = 
		    new Pair<String, GraphNodeData>(MAP_OUT_SCHEMA);
		/**
		 * Mapper for QuickMerge.
		 * 
		 * Input is read an avro file containing the nodes for the graph. 
		 * 
		 * For each input, we output the GraphNodeData keyed by the mertag so as to group
		 * nodes coming from the same read..
		 * 
		 */
		@Override
		public void map(GraphNodeData graph_data,
		    AvroCollector<Pair<String, GraphNodeData>> output, Reporter reporter)
						throws IOException {
			//node.fromNodeMsg(nodetxt.toString());
		  GraphNodeKMerTag tag = graph_data.getMertag();
		  
		  // Key is the read tag along with the chunk. 
			String mertag = tag.getReadTag() + "_" + tag.getChunk();
			out_pair.set(mertag, graph_data);
			output.collect(out_pair);
			reporter.incrCounter("Contrail", "nodes", 1);	
		}
	}

	/**
	 * In the reducer we try to merge nodes forming linear chains. The goal is to this initial
	 * merging efficiently by allowing all the nodes which could potentially be merged to loaded into
	 * memory at once. We do this by using the mapper to group nodes using the read tag and chunk. This heuristic
	 * has a good chance of grouping together nodes which can be merged because if K is << the read length we will
	 * get many edges from that read that can most likely be merged together. 
	 *
	 */
	private static class QuickMergeReducer extends 
	    AvroReducer<String, GraphNodeData, GraphNodeData> {
		private static int K = 0;
		public static boolean VERBOSE = false;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

    // domerge indicates whether we can merge the nodes.
    // domerge = 0  default/initial value
    // domerge = 1  chainlen >2 but allinmemory= false
    // domerge = 2  allinmemory = true
		enum DoMergeType {Init, ChainNotInMem, ChainInMem};
		
		
		/**
		 * Return true if all the destination nodes for node are stored in
		 * nodes_in_memory.
		 * 
		 * @param nodes_in_memory: A mapping from node_ids to nodes.
		 * @param node: Node to check if its destination nodes are in memory. 
		 * @return: True if the destination nodes are in nodes_in_memory.
		 */
		protected boolean allDestNodesInMemory(
		    Map<String, GraphNode> nodes_in_memory, GraphNode node){		  
  		// For each node in the chain we need to test whether
      // all the destinations for that node are in memory.
      boolean allinmemory = true;
      
      List<CharSequence> dest_node_ids = node.getAllDestIds();
      
      for (Iterator<CharSequence> it = dest_node_ids.iterator();
           it.hasNext();) {
          if (!nodes_in_memory.containsKey(it.next())) {
            allinmemory = false;
            return allinmemory;
          }
      }
      return allinmemory;
		}
		
		/**
		 * Merge the chain.
		 * @param nodes: Map containing the nodes in memory so we can access 
		 *   the nodes to be merged.
		 * @param rtnode: The node where we start the merge.
		 * @param mergedir: String representing the direction for the merge.
		 * @param stop_node_id: The id of the node at which we should stop the merge.
		 * @return
		 */
		protected MergeData mergeChain(
		    Map<String, GraphNode> nodes,
		    GraphNodeForQuickMerge rtnode, String mergedir,
		    CharSequence stop_node_id) {

      // JLEWI: Set first to be the tail in direction rtnode
      // since mergedir is the opposite direction, this is one link back.         
      TailInfoAvro first = rtnode.gettail(mergedir);
      GraphNodeForQuickMerge firstnode = 
          (GraphNodeForQuickMerge) nodes.get(first.id);

      // quick sanity check
      {
        TailInfoAvro firsttail = firstnode.gettail(DNAUtil.flip_dir(first.dir));
        if (!rtnode.getNodeId().equals(firsttail.id))
        {
          throw new IOException("Rtail->tail->tail != Rtail");
        }
      }
      Sequence sequence = new Sequence(DNAAlphabetFactory.create());
      sequence.readPackedBytes(
          rtnode.getCanonicalSourceKmer().getDna().Array(), 
          rtnode.getCanonicalSourceKmer().getLength());
      
      // merge string
      // JLEWI: mstr is the dna string.
      if (mergedir.equals("r")) {
        // JLEWI: if the merge direction is "r" than we need the
        // reverse complement. 
        sequence = DNAUtil.reverseComplement(sequence);
        rtnode.revreads();
      }
      
      
      //TailInfoAvro cur = new TailInfoAvro(first);
      TailInfoAvro current_tail = new TailInfoAvro(first);
      
      int mergelen = 0;

      GraphNodeForQuickMerge curnode =  
          (GraphNodeForQuickMerge) nodes.get(current_tail.id);

      int merlen = sequence.size() - K + 1;
      int covlen = merlen;

      double covsum = rtnode.getCoverage() * merlen;

      int shift = merlen;

      
      CharSequence lastid = current_tail.id;
      CharSequence lastdir = current_tail.dir;

      // LEWI it looks like the while loop is merging the sequence
      // until we get to ftail. The while loop walks the chain
      // of incoming edges that will end on node.
      // As walk the chain we set mstr to be the DNA sequence
      // resulting from mereging these nodes; similarlly we update
      // covsum and covlen. 
      while (!current_tail.id.equals(stop_node_id))
      {
        
        curnode = 
            (GraphNodeForQuickMerge) nodes.get(current_tail.id.toString());

        // if (VERBOSE) { System.err.println(curnode.toNodeMsg(true)); }

        // curnode can be deleted
        //curnode.setCustom(DONE, "2");
        curnode.deletable = true;
        mergelen++;

        // LEWI GET THE DNA STRING.
        //String bstr = curnode.str();
        Sequence sequence_to_add = new Sequence(DNAAlphabetFactory.create());
        sequence_to_add.readPackedBytes(
            curnode.getCanonicalSourceKmer().getDna().Array(), 
            curnode.getCanonicalSourceKmer().getLength());
        
        if (current_tail.dir.equals("r")) { 
//          bstr = Node.rc(bstr);
          sequence_to_add = DNAUtil.reverseComplement(sequence_to_add);
          curnode.revreads();
        }
      
        throw new RuntimeException("Left off here");
        // MERGE THE DNA SEQUENCE
        //mstr = Node.str_concat(mstr, bstr, K);
        sequence = DNAUtil.mergeSequences(sequence, sequence_to_add, K);
        merlen = sequence_to_add.size() - K + 1;
        covsum += curnode.cov() * merlen;
        covlen += merlen;

        rtnode.addreads(curnode, shift);
        shift += merlen;

        lastid = current_tail.id;
        lastdir = current_tail.dir;

        current_tail = curnode.gettail(lastdir);
      }
		}
		/**
		 * Reducer for QuickMerge.
		 * 
		 * Input:
		 * The key is a mertag. The value is a list of the serialized nodes with that mertag.
		 * 
		 */ 
  	@Override
    public void reduce(String  mertag, Iterable<GraphNodeData> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter)
            throws IOException
						{			
//			int saved    = 0;
//			int chains   = 0;
//			int cchains  = 0;
//			int totallen = 0;
//
//			String DONE = "DONE";			
//
//			// Load the nodes with the same key (mertag) into memory. The keys of this map will be 
  	    // the node_id. 
  	    Map<String, GraphNode> nodes = new HashMap<String, GraphNode>();
//
//			//VERBOSE = mertag.toString().equals("32_15326_10836_0_2") || mertag.toString().equals("22_5837_4190_0_2") || mertag.toString().equals("101_8467_7940_0_2");
//
  	    Iterator<GraphNodeData> iter = iterable.iterator();  	    
  			while(iter.hasNext()) {
  			  GraphNodeForQuickMerge node = new GraphNodeForQuickMerge();
  			  node.setData(iter.next());
//
//				if (VERBOSE)
//				{
//					//System.err.println(mertag + " " + nodestr);
//				}
//
//				if ((nodes.size() > 10) && (nodes.size() % 10 == 0))
//				{
//					//System.err.println("Common mer: " + mertag.toString() + " cnt:" + nodes.size());
//				}
//
  			  nodes.put(node.getNodeId(), node);
  			}
//
//			//if (nodes.size() > 10) { System.err.println("Loaded all nodes: " + nodes.size() + "\n"); }
//
//			// Now try to merge each node
//
  			int donecnt = 0;
  			for (String nodeid : nodes.keySet()) {
  			  donecnt++;
//				//if ((donecnt % 10) == 0) { System.err.println("Completed Merges: " + donecnt + "\n"); }
//
  			  GraphNodeForQuickMerge node = 
  			      (GraphNodeForQuickMerge)nodes.get(nodeid);
//
//				// The done field is used to keep track whether we've already processed the
//				// node with this id. 
  			  if (node.done) { 
  			    continue; 
  			  }
  			  node.done= true;

  	
  			  // Starting at node follow the chain of nodes in the reverse direction
  			  // of this edge. If we take the reverse complement of this chain
  			  // it will form a chain that terminates on node.
				TailInfoAvro rtail = TailInfoAvro.find_tail(nodes, node, "r");
				GraphNodeForQuickMerge rtnode = 
				    (GraphNodeForQuickMerge) nodes.get(rtail.id);
   				
				// Now follow the chain along the forward direction starting at node.
				// The end result is the chain
				//{r1,r2} >> rtail -> c1 -> c2 -> c3 -> node -> c4 -> c5 -> c6 -> ftail >> {f1,f2}
				// We catch cycles by looking for the ftail from rtail, not node.
				// A cycle occurs when we have a repeat along the path e.g
				// rtail --> R --> node -->R-->ftail which can occur in special 
				// circumstances. (What are the circumstances? rtail = ftail? repeated KMers?)  
				// So we need to check no cycles are encountered when merging from 
				// rtail to ftail. We need to do this when considering the path
				// from rtail to ftail because the paths rtail->node and node->ftail
				// may not contain the repeat. 
   				TailInfoAvro ftail = TailInfoAvro.find_tail(
   				    nodes, rtnode, rtail.dir.toString()); 
   				GraphNodeForQuickMerge ftnode = 
   				    (GraphNodeForQuickMerge)nodes.get(ftail.id);
//
   				rtnode.done = true;
   				ftnode.done = true;
//
  				int chainlen = 1 + ftail.dist;
  
  				chains++;
  				totallen += chainlen;
//
//				//VERBOSE = rtail.id.equals("HRJMRHHETMRJHSF") || rtail.id.equals("EECOECEOEECOECA") || rtail.id.equals("ECOECEOEECOECEK");
//				if (VERBOSE) { System.err.print(nodeid + " " + chainlen + " " + rtail + " " + ftail + " " + mertag.toString()); }
//				
				// domerge indicates whether we can merge the nodes.
				// domerge = 0  default/initial value
				// domerge = 1  chainlen >2 but allinmemory= false
				// domerge = 2  allinmemory = true
  				
				DoMergeType domerge = DoMergeType.Init;
				if (chainlen > 1)
				{
				  // We can only merge this node if the destination for this edge
				  // is in memory
				  if (allDestNodesInMemory(nodes, rtnode)) {
				    domerge = DoMergeType.ChainInMem;
				  } else if (chainlen >2) {
				    domerge = DoMergeType.ChainNotInMem;
				  }
				}
//
//				if (VERBOSE) { System.err.println(" domerge=" + domerge); }
//
				if (domerge != DoMergeType.Init)
				{
					chainlen--; // Replace the chain with 1 ftail
					if (domerge == DoMergeType.ChainNotInMem) { chainlen--; } // Need rtail too

					// start at the rtail, and merge until the ftail
					if (VERBOSE) 
					{ 
						System.err.println("[==");
						System.err.println(rtnode.toNodeMsg(true));
					}

					throw new RuntimeException("Code below hasn't been updated yet");


					// mergedir is the direction to merge relative to rtail
					String mergedir = rtail.dir;

					// Get data from the merge, e.g the combined sequence etc..
					// This function handles merging with all nodes except ftail.id
					MergeData merge_Data = mergeChain(
					    nodes, rtnode, mergedir);

					if (VERBOSE) { System.err.println(ftnode.toNodeMsg(true)); }
					if (VERBOSE) { System.err.println("=="); }

					// If we made it all the way to the ftail, 
					// see if we should do the final merge
					// LEWI: What does he mean do the final merge? 
					// Is the check for chainlen to check if we hit a cycle?
					if ((domerge == 2) && 
							(cur.id.equals(ftail.id)) && 
							(mergelen == (chainlen-1)))
					{
						// Lewi this is where we do the actual merge of the nodes.
						// In the previous loop (what is now mergechain) I think
						// he was just taking the data from each node that he needed
						// but he wasn't actually doing the merge. 
						mergelen++;
						rtnode.setCustom(DONE, "2");

						String bstr = ftnode.str();
						if (cur.dir.equals("r"))
						{
							bstr = Node.rc(bstr);
							// JLEWI: What is revreads doing?							
							ftnode.revreads();
						}

						mstr = Node.str_concat(mstr, bstr, K);

						merlen = bstr.length() - K + 1;
						covsum += ftnode.cov() * merlen;
						covlen += merlen;

						rtnode.addreads(ftnode, shift);

						// we want the same orientation for ftail as before
						if (cur.dir.equals("r")) { mstr = Node.rc(mstr); }
						ftnode.setstr(mstr);

						// Copy reads over
						ftnode.setR5(rtnode);
						if (cur.dir.equals("r")) { ftnode.revreads(); }

						ftnode.setCoverage((float) covsum / (float) covlen);

						// Update ftail's new neigbors to be rtail's old neighbors
						// Update the rtail neighbors to point at ftail
						// Update the can compress flags
						// Update threads

						// Clear the old links from ftnode in the direction of the chain
						ftnode.clearEdges(ftail.dir + "f");
						ftnode.clearEdges(ftail.dir + "r");

						// Now move the links from rtnode to ftnode
						for (String adj : Node.dirs)
						{
							String origdir = Node.flip_dir(rtail.dir) + adj;
							String newdir  = ftail.dir + adj;

							//System.err.println("Shifting " + rtail.id + " " + origdir);

							List<String> vl = rtnode.getEdges(origdir);

							if (vl != null)
							{
								for (String v : vl)
								{
									if (v.equals(rtail.id))
									{
										// Cycle on rtail
										if (VERBOSE) { System.err.println("Fixing rtail cycle"); }

										String cycled = ftail.dir;

										if (rtail.dir.equals(adj)) { cycled += Node.flip_dir(ftail.dir); }
										else                       { cycled += ftail.dir; }

										ftnode.addEdge(cycled, ftail.id);
									}
									else
									{
										ftnode.addEdge(newdir, v);

										Node vnode = nodes.get(v);
										vnode.replacelink(rtail.id, Node.flip_link(origdir),
												ftail.id, Node.flip_link(newdir));
									}
								}
							}
						}

						// Now move the can compresflag from rtnode into ftnode
						ftnode.setCanCompress(ftail.dir, rtnode.canCompress(Node.flip_dir(rtail.dir)));

						// Break cycles
						for (String dir : Node.dirs)
						{
							TailInfo next = ftnode.gettail(dir);

							if ((next != null) && next.id.equals(ftail.id))
							{
								if (VERBOSE) { System.err.println("Breaking tail " + ftail.id); }
								ftnode.setCanCompress("f", false);
								ftnode.setCanCompress("r", false);
							}
						}

						// Confirm there are no threads in $ftnode in $fdir
						List<String> threads = ftnode.getThreads();
						if (threads != null)
						{
							ftnode.clearThreads();

							for (String thread : threads)
							{
								String [] vals = thread.split(":"); // t, link, read

								if (!vals[0].substring(0,1).equals(ftail.dir))
								{
									ftnode.addThread(vals[0], vals[1], vals[2]);  
								}
							}
						}

						// Now copy over rtnodes threads in !$rdir
						threads = rtnode.getThreads();
						if (threads != null)
						{
							for (String thread : threads)
							{
								String [] vals = thread.split(":"); // t, link, read
								if (!vals[0].substring(0,1).equals(rtail.dir))
								{
									String et = ftail.dir + vals[0].substring(1);
									ftnode.addThread(et, vals[1], vals[2]);
								}

							}
						}

						if (VERBOSE) { System.err.println(ftnode.toNodeMsg(true)); }
						if (VERBOSE) { System.err.println("==]"); }
					}
					else
					{
						if (mergelen < chainlen)
						{
							System.err.println("Hit an unexpected cycle mergelen: " + mergelen + " chainlen: " + chainlen + " in " + rtnode.getNodeId() + " " + ftnode.getNodeId() + " mertag:" + mertag.toString());
							System.err.println(rtnode.toNodeMsg(true));
							System.err.println(ftnode.toNodeMsg(true));
							throw new IOException("Hit an unexpected cycle mergelen: " + mergelen + " chainlen: " + chainlen + " in " + rtnode.getNodeId() + " " + ftnode.getNodeId() + " mertag:" + mertag.toString());
						}

						if (mergedir.equals("r")) 
						{ 
							mstr = Node.rc(mstr);
							rtnode.revreads();
						}

						rtnode.setstr(mstr);

						rtnode.setCoverage((float) covsum / (float) covlen);

						String mergeftaildir = lastdir;
						if (!lastdir.equals(mergedir)) { mergeftaildir = Node.flip_dir(mergeftaildir); }

						// update rtail->first with rtail->ftail link
						rtnode.replacelink(first.id, mergedir + first.dir, 
								ftail.id, mergeftaildir + cur.dir);

						ftnode.replacelink(lastid, Node.flip_link(lastdir+cur.dir),
								rtail.id,Node.flip_link(mergeftaildir + cur.dir));

						if (curnode.getThreads() != null)
						{
							//throw new IOException("ERROR: curnode has threads " + curnode.toNodeMsg(true));
							curnode.cleanThreads();
						}

						if (VERBOSE) { System.err.println(rtnode.toNodeMsg(true)); }
						if (VERBOSE) { System.err.println("==]"); }
					}

					saved  += mergelen;
					cchains++;
				}
  			}
//
//			for(String nodeid : nodes.keySet())
//			{
//				Node node = nodes.get(nodeid);
//				if (node.hasCustom(DONE) && node.getCustom(DONE).get(0).equals("1"))
//				{
//					output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
//				}
//			}
//
//			reporter.incrCounter("Contrail", "chains",        chains);
//			reporter.incrCounter("Contrail", "cchains",       cchains);
//			reporter.incrCounter("Contrail", "totalchainlen", totallen);
//			reporter.incrCounter("Contrail", "saved",         saved);
		}
	}




	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
	  throw new NotImplementedException();
//		sLogger.info("Tool name: QuickMerge");
//		sLogger.info(" - input: "  + inputPath);
//		sLogger.info(" - output: " + outputPath);
//
//		JobConf conf = new JobConf(Stats.class);
//		conf.setJobName("QuickMerge " + inputPath + " " + ContrailConfig.K);
//
//		ContrailConfig.initializeConfiguration(conf);
//
//		FileInputFormat.addInputPath(conf, new Path(inputPath));
//		FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//		conf.setInputFormat(TextInputFormat.class);
//		conf.setOutputFormat(TextOutputFormat.class);
//
//		conf.setMapOutputKeyClass(Text.class);
//		conf.setMapOutputValueClass(Text.class);
//
//		conf.setOutputKeyClass(Text.class);
//		conf.setOutputValueClass(Text.class);
//
//		conf.setMapperClass(QuickMergeMapper.class);
//		conf.setReducerClass(QuickMergeReducer.class);
//
//		//delete the output directory if it exists already
//		FileSystem.get(conf).delete(new Path(outputPath), true);
//
//		return JobClient.runJob(conf);
	}

	@Override
	protected int run() throws Exception {
	  throw new NotImplementedException();
	}
	
	public int run(String[] args) throws Exception 
	{

		Option kmer = new Option("k","k",true,"k. The length of each kmer to use.");
		Option input = new Option("input","input",true,"The directory containing the input (i.e the output of BuildGraph.)");
		Option output = new Option("output","output",true,"The directory where the output should be written to.");

		Options options = new Options();
		options.addOption(kmer);
		options.addOption(input);
		options.addOption(output);

		CommandLineParser parser = new GnuParser();

		CommandLine line = parser.parse( options, args );


		if (!line.hasOption("input") || !line.hasOption("output") || !line.hasOption("k")){
			System.out.println("ERROR: Missing required arguments");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "QuickMerge", options );
		}

		String inputPath  = line.getOptionValue("input");
		String outputPath = line.getOptionValue("output");
		ContrailConfig.K = Integer.parseInt(line.getOptionValue("k"));

		ContrailConfig.hadoopBasePath = "foo";
		ContrailConfig.hadoopReadPath = "foo";

		run(inputPath, outputPath);
		//TODO: do we need to parse the other options?
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new QuickMergeAvro(), args);
		System.exit(res);
	}
}
