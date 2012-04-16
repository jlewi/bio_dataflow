package contrail.avro;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import contrail.Node;
import contrail.TailInfo;
import contrail.graph.EdgeDirection;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.KMerEdge;
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
          new CompressibleMapOutput.getSchema());

  /**
   * Define the schema for the reducer output. The output is a graph node
   * decorated with information about whether its compressible.
   */
  public static final Schema REDUCE_OUT_SCHEMA = 
      (new CompressibleOutput()).getSchema();
    
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
    private static GraphNode node = new GraphNode();
    
    // Output pair to use for the mapper.
    private static Pair<CharSequence, CompressibleMapOutput> out_pair = new 
        Pair<CharSequence, CompressibleMapOutput>(MAP_OUT_SCHEMA);
    
    // Message to use; we use a static instance to avoid the cost
    // of recreating one everytime we need one.
    private static CompressibleMessage message = new CompressibleMessage();
    
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
     * also send messages to a node.
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
          
          message.setFromNodeId(node.getNodeId());
          
          // TODO(jlewi): Check that this is correct. We probably want to
          // send the direction for the edge. Since the strand is always forward.
          message.setFromStrand(DNAStrand.FORWARD);
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
}
