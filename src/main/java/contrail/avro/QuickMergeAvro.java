// Author: Michael Schatz, Jeremy Lewi
package contrail.avro;

import contrail.sequences.CompressedSequence;
import contrail.CompressedRead;
import contrail.ContrailConfig;

import contrail.Stats;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

import contrail.avro.BuildGraphAvro.BuildGraphMapper;
import contrail.avro.BuildGraphAvro.BuildGraphReducer;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.LinearChainWalker;
import contrail.graph.NodeMerger;
import contrail.graph.R5Tag;
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
import org.apache.avro.mapred.AvroJob;
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
  
//  protected void initializeDefaultOptions() {
//    super.initializeDefaultOptions();
////    default_options.put("TRIM3", new Long(0));
////    default_options.put("TRIM5", new Long(0));
////    default_options.put("MAXR5", new Long(250));
////    default_options.put("MAXTHREADREADS", new Long(250));
////    default_options.put("RECORD_ALL_THREADS", new Long(0));
//  }
  
  /**
   * Get the options required by this stage.
   */
  protected List<Option> getCommandLineOptions() {
    List<Option> options = super.getCommandLineOptions();

//    // Default values.
//    // hard trim
//    long TRIM3 = (Long)default_options.get("TRIM3");
//    long TRIM5 = (Long)default_options.get("TRIM5");
//    long MAXR5 = (Long)default_options.get("MAXR5");
//    long MAXTHREADREADS = (Long)default_options.get("MAXTHREADREADS");
//    long  RECORD_ALL_THREADS = (Long)default_options.get("RECORD_ALL_THREADS");
    // Add options specific to this stage.
    options.add(OptionBuilder.withArgName("k").hasArg().withDescription(
        "Graph nodes size [required]").create("k"));
//    options.add(OptionBuilder.withArgName(
//        "max reads").hasArg().withDescription(
//            "max reads starts per node (default: " + MAXR5 +")").create(
//                "maxr5"));
//    options.add(OptionBuilder.withArgName("3' bp").hasArg().withDescription(
//        "Chopped bases (default: " + TRIM3 +")").create("trim3"));
//    options.add(OptionBuilder.withArgName("5' bp").hasArg().withDescription(
//        "Chopped bases (default: " + TRIM5 + ")").create("trim5"));
//    options.add(new Option(
//        "record_all_threads",  "record threads even on non-branching nodes"));

    options.addAll(ContrailOptions.getInputOutputPathOptions());
    return options;
  }
  
  
	public static class QuickMergeMapper extends 
	  AvroMapper<GraphNodeData, Pair<CharSequence, GraphNodeData>> {
		private static GraphNode node = new GraphNode();

		private static Pair<CharSequence, GraphNodeData> out_pair = 
		    new Pair<CharSequence, GraphNodeData>(MAP_OUT_SCHEMA);
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
		    AvroCollector<Pair<CharSequence, GraphNodeData>> output, Reporter reporter)
						throws IOException {
		  		  
		  // The key is the read tag along with the chunk. 
		  // We want to group KMers coming from the same read as they are likely
		  // to form linear chains. We need to use the chunk as well because
		  // the chunk segments tags based on repeat KMers.
		  
		  String mertag = graph_data.getMertag().getReadTag().toString() + "_" +
		                  graph_data.getMertag().getChunk();
		  
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
	public static class QuickMergeReducer extends 
	    AvroReducer<String, GraphNodeData, GraphNodeData> {
		private static int K = 0;
		public static boolean VERBOSE = false;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}
		
		/**
		 * Reducer for QuickMerge.
		 */ 
  	@Override
    public void reduce(String  mertag, Iterable<GraphNodeData> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter)
            throws IOException
						{			
//			int saved    = 0;
//			int chains   = 0;
  	    
	    // The number of compressed chains.
	    int num_compressed_chains  = 0;
	    // Total number of nodes used to form the compressed chains.
	    int num_nodes_in_compressed_chains = 0;
	    // Load the nodes into memory.
	    Map<String, GraphNode> nodes = new HashMap<String, GraphNode>();
	    
	    Iterator<GraphNodeData> iter = iterable.iterator();  	    
			while(iter.hasNext()) {
			  GraphNode node = new GraphNode();
			  node.setData(iter.next());
			  nodes.put(node.getNodeId(), node);
			}
			
			// Create a list of the nodes to process.
			// We need to make a copy of nodes.keySet otherwise when we remove
			// an entry from the set we remove it from the hashtable.
			Set<String> nodes_to_process = new HashSet<String>();
			nodes_to_process.addAll(nodes.keySet());
			
			// List of the nodes to output.
			//Set<String> nodes_to_output = new HashSet<String>();
			
			while (nodes_to_process.size() > 0) {
			  String nodeid = nodes_to_process.iterator().next();
			  nodes_to_process.remove(nodeid);

			  GraphNode start_node = nodes.get(nodeid);

			  // Find a chain if any to merge.
			  QuickMergeUtil.NodesToMerge nodes_to_merge = 
			      QuickMergeUtil.findNodesToMerge(nodes, start_node);
			  
			  if (nodes_to_merge.start_terminal == null &&
			      nodes_to_merge.end_terminal == null) {
			
			    //nodes_to_output.add(nodeid);
			    // Node doesn't have a tail we can merge.
			    continue;
			  }
			  
			  // Merge the nodes.
			  QuickMergeUtil.ChainMergeResult merge_result = 
			      QuickMergeUtil.mergeLinearChain(nodes, nodes_to_merge, K - 1);
			  
			  num_compressed_chains += 1;
			  num_nodes_in_compressed_chains += merge_result.merged_nodeids.size();
			  
			  // Remove all the merged nodes from the list of ids to process.
			  nodes_to_process.removeAll(nodes_to_merge.nodeids_visited);
			  
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
			reporter.incrCounter(
			    "Contrail", "num_compressed_chains",  num_compressed_chains);
			//reporter.incrCounter("Contrail", "cchains",       cchains);
			reporter.incrCounter(
			    "Contrail", "num_nodes_in_compressed_chains", 
			    num_nodes_in_compressed_chains);
//			reporter.incrCounter("Contrail", "saved",         saved);
		}
	}

  protected void parseCommandLine(CommandLine line) {
    super.parseCommandLine(line);       

    if (line.hasOption("k")) {
      stage_options.put("K", Long.valueOf(line.getOptionValue("k"))); 
    }    
    if (line.hasOption("inputpath")) { 
      stage_options.put("inputpath", line.getOptionValue("inputpath")); 
    }
    if (line.hasOption("outputpath")) { 
      stage_options.put("outputpath", line.getOptionValue("outputpath")); 
    }
  }

  public int run(String[] args) throws Exception 
  {
    sLogger.info("Tool name: QuickMergeAvro");
    parseCommandLine(args);   
    return run();
  }

//	public RunningJob run(String inputPath, String outputPath) throws Exception
//	{ 
//	  throw new NotImplementedException();
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
//	}

	@Override
	protected int run() throws Exception {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    long K = (Long)stage_options.get("K");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    JobConf conf = new JobConf(QuickMergeAvro.class);
    conf.setJobName("QuickMergeAvro " + inputPath + " " + K);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, QuickMergeAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, QuickMergeAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, QuickMergeMapper.class);
    AvroJob.setReducerClass(conf, QuickMergeReducer.class);

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
    JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();

    float diff = (float) (((float) (endtime - starttime)) / 1000.0);

    System.out.println("Runtime: " + diff + " s");
    return 0;
	}
	
//	public int run(String[] args) throws Exception 
//	{
//
//		Option kmer = new Option("k","k",true,"k. The length of each kmer to use.");
//		Option input = new Option("input","input",true,"The directory containing the input (i.e the output of BuildGraph.)");
//		Option output = new Option("output","output",true,"The directory where the output should be written to.");
//
//		Options options = new Options();
//		options.addOption(kmer);
//		options.addOption(input);
//		options.addOption(output);
//
//		CommandLineParser parser = new GnuParser();
//
//		CommandLine line = parser.parse( options, args );
//
//
//		if (!line.hasOption("input") || !line.hasOption("output") || !line.hasOption("k")){
//			System.out.println("ERROR: Missing required arguments");
//			HelpFormatter formatter = new HelpFormatter();
//			formatter.printHelp( "QuickMerge", options );
//		}
//
//		String inputPath  = line.getOptionValue("input");
//		String outputPath = line.getOptionValue("output");
//		ContrailConfig.K = Integer.parseInt(line.getOptionValue("k"));
//
//		ContrailConfig.hadoopBasePath = "foo";
//		ContrailConfig.hadoopReadPath = "foo";
//
//		run(inputPath, outputPath);
//		//TODO: do we need to parse the other options?
//		return 0;
//	}

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new QuickMergeAvro(), args);
		System.exit(res);
	}
}
