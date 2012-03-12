// Author: Michael Schatz, Jeremy Lewi
package contrail.avro;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

import java.io.IOException;

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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class QuickMergeAvro extends Stage {	
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
   * Get the options required by this stage.
   */
  protected List<Option> getCommandLineOptions() {
    List<Option> options = super.getCommandLineOptions();
    options.addAll(ContrailOptions.getInputOutputPathOptions());
    
    // Add options specific to this stage.
    options.add(OptionBuilder.withArgName("k").hasArg().withDescription(
        "Graph nodes size [required]").create("k"));
 
    return options;
  }
  
  
	public static class QuickMergeMapper extends 
	  AvroMapper<GraphNodeData, Pair<CharSequence, GraphNodeData>> {
		
		private static Pair<CharSequence, GraphNodeData> out_pair = 
		    new Pair<CharSequence, GraphNodeData>(MAP_OUT_SCHEMA);
		/**
		 * Mapper for QuickMerge.
		 * 
		 * Input is an avro file containing the nodes for the graph. 		 
		 * For each input, we output the GraphNodeData keyed by the mertag so as to 
		 * group nodes coming from the same read.
		 */
		@Override
		public void map(GraphNodeData graph_data,
		    AvroCollector<Pair<CharSequence, GraphNodeData>> output, 
		    Reporter reporter) throws IOException {		  		  
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
	    AvroReducer<CharSequence, GraphNodeData, GraphNodeData> {
		private static int K = 0;
		public static boolean VERBOSE = false;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}
		
		/**
		 * Reducer for QuickMerge.
		 */ 
  	@Override
    public void reduce(CharSequence  mertag, Iterable<GraphNodeData> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter)
            throws IOException {			
	    // The number of compressed chains.
	    int num_compressed_chains  = 0;
	    // Total number of nodes used to form the compressed chains.
	    int num_nodes_in_compressed_chains = 0;
	    
	    // Load the nodes into memory.
	    Map<String, GraphNode> nodes = new HashMap<String, GraphNode>();
	    Iterator<GraphNodeData> iter = iterable.iterator();
	    
			while(iter.hasNext()) {			  
			  // We need to make a copy of GraphNodeData because iterable
			  // will reuse the same instance when next is called.
			  GraphNodeData value = iter.next();
			  GraphNode node = new GraphNode(value);
			  node = node.clone();
			  nodes.put(node.getNodeId().toString(), node);
			}
			
			// Create a list of the nodes to process. We need to make a copy of 
			// nodes.keySet otherwise when we remove an entry from the set we remove 
			// it from the hashtable.
			Set<String> nodes_to_process = new HashSet<String>();
			nodes_to_process.addAll(nodes.keySet());
			
			while (nodes_to_process.size() > 0) {
			  String nodeid = nodes_to_process.iterator().next();
			  nodes_to_process.remove(nodeid);

			  GraphNode start_node = nodes.get(nodeid);

			  if (start_node == null) {
			    throw new RuntimeException("Start node shouldn't be null");
			  }
			  
			  // Find a chain if any to merge.
			  QuickMergeUtil.NodesToMerge nodes_to_merge = 
			      QuickMergeUtil.findNodesToMerge(nodes, start_node);

        // Remove all the nodes visited from the list of ids to process.			  
        nodes_to_process.removeAll(nodes_to_merge.nodeids_visited);
        
			  if (nodes_to_merge.start_terminal == null &&
			      nodes_to_merge.end_terminal == null) {
			    continue;
			  }
			  
			  // Merge the nodes.
			  QuickMergeUtil.ChainMergeResult merge_result = 
			      QuickMergeUtil.mergeLinearChain(nodes, nodes_to_merge, K - 1);
			  
			  num_compressed_chains += 1;
			  num_nodes_in_compressed_chains += merge_result.merged_nodeids.size();
			  			  
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

			reporter.incrCounter(
			    "Contrail", "num_compressed_chains",  num_compressed_chains);
			reporter.incrCounter(
			    "Contrail", "num_nodes_in_compressed_chains", 
			    num_nodes_in_compressed_chains);
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

  public int run(String[] args) throws Exception {
    sLogger.info("Tool name: QuickMergeAvro");
    parseCommandLine(args);   
    return run();
  }

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

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new QuickMergeAvro(), args);
		System.exit(res);
	}
}
