package contrail.avro;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.cli.Option;
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

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeDirectionUtil;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.TailData;
import contrail.sequences.DNAStrand;

/**
 * This mapreduce stage marks nodes which can be compressed. A node
 * can be compressed if its part of a linear chain; i.e there is a single
 * path through that node. 
 */
public class CompressibleAvro extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      CompressibleAvro.class);
 
  public static final Schema graph_node_data_schema = 
      (new GraphNodeData()).getSchema();

  /**
   * Define the schema for the mapper output. The keys will be a string 
   * representing the node id. The value will be an instance of 
   * CompressibleMapOutput which contains either a node in the graph or
   * a message to a node.
   */
  public static final Schema MAP_OUT_SCHEMA = 
      Pair.getPairSchema(
          Schema.create(Schema.Type.STRING), 
          new CompressibleMapOutput().getSchema());

  /**
   * Define the schema for the reducer output. The output is a graph node
   * decorated with information about whether its compressible.
   */
  public static final Schema REDUCE_OUT_SCHEMA = 
      (new CompressibleNodeData()).getSchema();
    
  /**
   * Get the options required by this stage.
   */
  protected List<Option> getCommandLineOptions() {
    List<Option> options = super.getCommandLineOptions();
    options.addAll(ContrailOptions.getInputOutputPathOptions());    
     return options;
  }
    
  public static class CompressibleMapper extends 
    AvroMapper<GraphNodeData, Pair<CharSequence, CompressibleMapOutput>> {
    
    // Node used to process the graphnode data.
    private GraphNode node = new GraphNode();
    
    // Output pair to use for the mapper.
    private Pair<CharSequence, CompressibleMapOutput> out_pair = new 
        Pair<CharSequence, CompressibleMapOutput>(MAP_OUT_SCHEMA);
    
    // Message to use; we use a static instance to avoid the cost
    // of recreating one everytime we need one.
    private CompressibleMessage message = new CompressibleMessage();
    
    public void configure(JobConf job) {
      out_pair.value(new CompressibleMapOutput());
    }
    
    /**
     * Clear the fields in the record.
     * @param record
     */
    protected void clearCompressibleMapOutput(CompressibleMapOutput record) {
      record.setNode(null);
      record.setMessage(null);
    }
    
    /**
     * Mapper for Compressible.
     * 
     * Input is an avro file containing the nodes for the graph.     
     * For each input, we output the GraphNodeData keyed by string. 
     * We check if this node is part of a linear chain and if it is
     * we send messages to the neighbors letting them know that they
     * are connected to a node which is compressible.
     */
    @Override
    public void map(GraphNodeData graph_data,
        AvroCollector<Pair<CharSequence, CompressibleMapOutput>> output, 
        Reporter reporter) throws IOException {
          
      // Set both fields of comp
      node.setData(graph_data);
      
      CompressibleMapOutput map_output = out_pair.value();
      
      // We only consider the forward strand because the reverse strand
      // is the mirror image.
      for (EdgeDirection direction: EdgeDirection.values()) {            
        TailData tail = node.getTail(DNAStrand.FORWARD, direction);
        
        if (tail != null) {
          // We have a tail in this direction.
          if (tail.terminal.nodeId.equals(node.getNodeId())) {
            // Cycle so continue.
            continue; 
          }
          
          reporter.incrCounter("Contrail", "remotemark", 1);
          
          // Send a message to the neighbor telling it this node is compressible.
          clearCompressibleMapOutput(map_output);
          out_pair.key(tail.terminal.nodeId);
          
          // TODO(jlewi): We don't store the strand of the destination
          // node. Is this ok? 
          message.setFromNodeId(node.getNodeId());          
          message.setFromDirection(direction);
          map_output.setMessage(message);
          output.collect(out_pair);
        }
      }
      
      // Output the node in the graph.
      out_pair.key(node.getNodeId());
      clearCompressibleMapOutput(map_output);
      map_output.setNode(graph_data);
      out_pair.value(map_output);    
      output.collect(out_pair);    
      reporter.incrCounter("Contrail", "nodes", 1);  
    }
  }
  
  public static class CompressibleReducer extends 
      AvroReducer<CharSequence, CompressibleMapOutput, CompressibleNodeData> {    
    private static GraphNode node = new GraphNode(); 
    
    
    // We store the nodes sending messages in two sets, one corresponding
    // to messages from incoming edges, and one from outgoing edges.
    // Direction is relative to the forward strand.
    private static HashSet<String> incoming_nodes = new 
        HashSet<String>();
    private static HashSet<String> outgoing_nodes = new 
        HashSet<String>();
    
    // The output from the reducer is a node annotated with information
    // about whether its attached to compressible nodes or not.
    private static CompressibleNodeData annotated_node = 
        new CompressibleNodeData();
    
    // Clear the data in the node. 
    private void clearAnnotatedNode(CompressibleNodeData node) {
      node.setNode(null);
      node.getCompressibleDirections().clear();
    }
    
    public void configure(JobConf job) {
      // Initialize the list in annotated_node;
      annotated_node.setCompressibleDirections(new ArrayList<EdgeDirection>());
    }
    
    @Override
    public void reduce(
        CharSequence  node_id, Iterable<CompressibleMapOutput> iterable,
        AvroCollector<CompressibleNodeData> collector, Reporter reporter)
            throws IOException {  
      
      boolean has_node = false;
      incoming_nodes.clear();
      outgoing_nodes.clear();
      clearAnnotatedNode(annotated_node);
      
      Iterator<CompressibleMapOutput> iter = iterable.iterator();
      
      while (iter.hasNext()) {
        // We need to make copies because the iterable returned by hadoop
        // tends to reuse objects.
        CompressibleMapOutput output = iter.next();        
        if (output.getMessage() != null && output.getNode() != null) {
          // This is an error only 1 should be set.
          reporter.incrCounter(
              "Contrail", "compressible-error-message-and-node-set", 1);
          continue;
        }
        if (output.getNode() != null ) {
          if (has_node) {
            reporter.incrCounter(
                "Contrail", "compressible-error-node-duplicated", 1);
            throw new RuntimeException(
                "Error: Node " + node_id + " appeared multiple times.");
          }
          node.setData(output.getNode());
          node = node.clone();
          has_node = true;
        }
        
        if (output.getMessage() != null) {
          CompressibleMessage msg = (CompressibleMessage)
              SpecificData.get().deepCopy(
                  output.getMessage().getSchema(), output.getMessage());
          if (msg.getFromDirection() == EdgeDirection.INCOMING) {
            incoming_nodes.add(msg.getFromNodeId().toString());
          } else {
            outgoing_nodes.add(msg.getFromNodeId().toString());
          }
        }
      }
      
      if (!has_node) {
        throw new RuntimeException(
            "Error: No node provided for node: " + node_id);
      }

      annotated_node.setNode(node.getData());
      
      for (EdgeDirection process_direction: EdgeDirection.values()) {
        // A node(src) sends a message to this node(target) if it has a single 
        // incoming/outgoing edge to the target. If target also has a single
        // edge which corresponds to the src then we mark the node as being
        // compressible in that direction.
        HashSet<String> message_nodes; 
        if (process_direction == EdgeDirection.INCOMING) {
          message_nodes = incoming_nodes;
        } else {
          message_nodes = outgoing_nodes;
        }
        // We always deal with the forward strand.
        // We need to flip the direction of the edge because if the edge
        // is an incoming for the source node then its an outgoing edge for
        // this node.
        EdgeDirection compressible_direction = 
            EdgeDirectionUtil.flip(process_direction);
        TailData tail_data = node.getTail(
            DNAStrand.FORWARD, compressible_direction);
        
        if (tail_data == null) {
          // There's no tail in this direction so we can't compress.
          continue;
        }
        
        // Sanity check since we have a tail in this direction we should
        // have at most a single message.
        if (message_nodes.size() > 1) {
          // TODO(jlewi): We use an exception for now because we should
          // treat this as an unrecoverable error.
          throw new RuntimeException(
              "Node: " + node.getNodeId() + "has a tail in direction " + 
              EdgeDirectionUtil.flip(process_direction) + " but has more " + 
              "than 1 message in this direction. Number of messages is " + 
              message_nodes.size());
        }
        // If the tail in this direction is the forward strand of
        // the source then we can compress the chain in this direction.          
        if (tail_data.terminal.strand == DNAStrand.FORWARD && 
            message_nodes.contains(tail_data)) {
          annotated_node.getCompressibleDirections().add(
              compressible_direction);
          reporter.incrCounter("Contrail", "compressible", 1);
        }         
      }            
      collector.collect(annotated_node);
    }
  }

  public int run(String[] args) throws Exception {
    sLogger.info("Tool name: QuickMergeAvro");
    parseCommandLine(args);   
    return run();
  }

  @Override
  protected int run() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasOptionsOrDie(required_args);
    
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    JobConf conf = new JobConf(QuickMergeAvro.class);
    conf.setJobName("CompressibleAvro " + inputPath);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, graph_data.getSchema());
    AvroJob.setMapOutputSchema(conf, CompressibleAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, CompressibleAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, CompressibleMapper.class);
    AvroJob.setReducerClass(conf, CompressibleReducer.class);

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
      JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();
  
      float diff = (float) (((float) (endtime - starttime)) / 1000.0);
  
      System.out.println("Runtime: " + diff + " s");
    }
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CompressibleAvro(), args);
    System.exit(res);
  }
}
