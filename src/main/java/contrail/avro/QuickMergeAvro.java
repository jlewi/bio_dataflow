package contrail.avro;

import contrail.sequences.CompressedSequence;
import contrail.ContrailConfig;

import contrail.Stats;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.LinearChainWalker;
import contrail.graph.NodeMerger;
import contrail.graph.TailData;

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
		  // TODO(jlewi):
		  String mertag = "";
		  throw new NotImplementedException("I removed the field getmertag so now I need to find the smallest value for the tag");
		  //GraphNodeKMerTag tag = graph_data.getMertag();
		  
		  // Key is the read tag along with the chunk. 
		  // We want to group KMers coming from the same read as they are likely
		  // to form linear chains. We need to use the chunk as well because
		  // the chunk segments tags based on repeat KMers. If we didn't use
		  // the chunk we would create cycles which wouldn't know how to merge.
			//String mertag = tag.getReadTag() + "_" + tag.getChunk();
			out_pair.set(mertag, graph_data);
			output.collect(out_pair);
			reporter.incrCounter("Contrail", "nodes", 1);	
		}
	}

	/**
	 * In the reducer we try to merge nodes forming linear chains. 
	 * 
	 * By construction, most of the nodes coming from a given read will form a
	 * linear chain. We do the merge by using the mapper to group nodes using the 
	 * read tag and chunk. This heuristic has a good chance of grouping together 
	 * nodes which can be merged because if K is << the read length we will
	 * get many edges from each read that can most likely be merged together. 
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
		 * Return true if all the terminals are stored in memory.
		 * 
		 * @param nodes_in_memory: A mapping from node_ids to nodes.
		 * @param terminals: Terminals to check if they aree in memory. 
		 * @return: True if the terminals are in nodes_in_memory.
		 */
//		protected boolean allTerminalsInMemory(
//		    Map<String, GraphNode> nodes_in_memory, List<EdgeTerminal> terminals){		  
//  		// For each node in the chain we need to test whether
//      // all the destinations for that node are in memory.
//      boolean allinmemory = true;
//                 
//      for (EdgeTerminal terminal: terminals) {
//          if (!nodes_in_memory.containsKey(terminal.nodeId)) {
//            allinmemory = false;
//            return allinmemory;
//          }
//      }
//      return allinmemory;
//		}
//		
		/**
		 * Merge the chain.
		 * @param nodes: Map containing the nodes in memory so we can access 
		 *   the nodes to be merged.
		 * @param rtnode: The node where we start the merge.
		 * @param mergedir: String representing the direction for the merge.
		 * @param stop_node_id: The id of the node at which we should stop the merge.
		 * @return
		 */
//		protected MergeData mergeChain(
//		    Map<String, GraphNode> nodes,
//		    GraphNodeForQuickMerge rtnode, String mergedir,
//		    CharSequence stop_node_id) {
//
//      // JLEWI: Set first to be the tail in direction rtnode
//      // since mergedir is the opposite direction, this is one link back.         
//      TailInfoAvro first = rtnode.gettail(mergedir);
//      GraphNodeForQuickMerge firstnode = 
//          (GraphNodeForQuickMerge) nodes.get(first.id);
//
//      // quick sanity check
//      {
//        TailInfoAvro firsttail = firstnode.gettail(DNAUtil.flip_dir(first.dir));
//        if (!rtnode.getNodeId().equals(firsttail.id))
//        {
//          throw new IOException("Rtail->tail->tail != Rtail");
//        }
//      }
//      Sequence sequence = new Sequence(DNAAlphabetFactory.create());
//      sequence.readPackedBytes(
//          rtnode.getCanonicalSourceKmer().getDna().Array(), 
//          rtnode.getCanonicalSourceKmer().getLength());
//      
//      // merge string
//      // JLEWI: mstr is the dna string.
//      if (mergedir.equals("r")) {
//        // JLEWI: if the merge direction is "r" than we need the
//        // reverse complement. 
//        sequence = DNAUtil.reverseComplement(sequence);
//        rtnode.revreads();
//      }
//      
//      
//      //TailInfoAvro cur = new TailInfoAvro(first);
//      TailInfoAvro current_tail = new TailInfoAvro(first);
//      
//      int mergelen = 0;
//
//      GraphNodeForQuickMerge curnode =  
//          (GraphNodeForQuickMerge) nodes.get(current_tail.id);
//
//      int merlen = sequence.size() - K + 1;
//      int covlen = merlen;
//
//      double covsum = rtnode.getCoverage() * merlen;
//
//      int shift = merlen;
//
//      
//      CharSequence lastid = current_tail.id;
//      CharSequence lastdir = current_tail.dir;
//
//      // LEWI it looks like the while loop is merging the sequence
//      // until we get to ftail. The while loop walks the chain
//      // of incoming edges that will end on node.
//      // As walk the chain we set mstr to be the DNA sequence
//      // resulting from mereging these nodes; similarlly we update
//      // covsum and covlen. 
//      while (!current_tail.id.equals(stop_node_id))
//      {
//        
//        curnode = 
//            (GraphNodeForQuickMerge) nodes.get(current_tail.id.toString());
//
//        // if (VERBOSE) { System.err.println(curnode.toNodeMsg(true)); }
//
//        // curnode can be deleted
//        //curnode.setCustom(DONE, "2");
//        curnode.deletable = true;
//        mergelen++;
//
//        // LEWI GET THE DNA STRING.
//        //String bstr = curnode.str();
//        Sequence sequence_to_add = new Sequence(DNAAlphabetFactory.create());
//        sequence_to_add.readPackedBytes(
//            curnode.getCanonicalSourceKmer().getDna().Array(), 
//            curnode.getCanonicalSourceKmer().getLength());
//        
//        if (current_tail.dir.equals("r")) { 
////          bstr = Node.rc(bstr);
//          sequence_to_add = DNAUtil.reverseComplement(sequence_to_add);
//          curnode.revreads();
//        }
//      
//        throw new RuntimeException("Left off here");
//        // MERGE THE DNA SEQUENCE
//        //mstr = Node.str_concat(mstr, bstr, K);
//        sequence = DNAUtil.mergeSequences(sequence, sequence_to_add, K);
//        merlen = sequence_to_add.size() - K + 1;
//        covsum += curnode.cov() * merlen;
//        covlen += merlen;
//
//        rtnode.addreads(curnode, shift);
//        shift += merlen;
//
//        lastid = current_tail.id;
//        lastdir = current_tail.dir;
//
//        current_tail = curnode.gettail(lastdir);
//      }
//		}
		
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
//			// Load the nodes with the same key (mertag) into memory. 
  	    // The keys of this map will be 
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
  			// TODO(jlewi) To mark a node as done we could just remove it from 
  			// a set of nodeids still to process
  			
  			// Create a list of the nodes to process.
  			Set<String> nodes_to_process = nodes.keySet(); 
  			while (nodes_to_process.size() > 0) {
  			  String nodeid = nodes_to_process.iterator().next();
  			  nodes_to_process.remove(nodeid);
  			  donecnt++;

  			  GraphNode start_node = nodes.get(nodeid);
  			  // Find a chain if any to merge.
  			  QuickMergeUtil.NodesToMerge nodes_to_merge = 
  			      QuickMergeUtil.findNodesToMerge(nodes, start_node);
  			  
  			  if (nodes_to_merge == null) {
  			    // Node doesn't have a tail we can merge.
  			    continue;
  			  }
  			  
  			  // Merge the nodes.
  			  QuickMergeUtil.MergeResult merge_result = 
  			      QuickMergUtil.mergeLinearChain(nodes, nodes_to_merge);
  			  
  			  // Remove all the merged nodes from the list of ids to proces
  			  nodes_to_process.removeAll(merge_result.merged_nodeids);
  			  
  			  // We should remove the last terminal even if it wasn't merged
  			  // because nothing can be done with it.
  			  if (!nodes_to_merge.include_final_terminal) {
  			    nodes_to_process.remove(nodes_to_merge.end_terminal.nodeId);
  			  }
  			  
  			  // Remove the merged nodes from nodes because these should not
  			  // be outputted.
  			  for (String merged_nodeid: merge_result.merged_nodeids){
  			     nodes.remove(merged_nodeid);  
  			  }
  			  // Add the newly merged node to the list of nodes.
  			  nodes.put(merge_result.merged_node.getNodeId(), 
  			            merge_result.merged_node);
  			}

  			// Output all the remaining nodes.
			for(String nodeid : nodes.keySet()) {				
				collector.collect(nodes.get(nodeid).getData());				
			}
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
