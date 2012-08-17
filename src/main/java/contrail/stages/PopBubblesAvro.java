package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.FindBubblesOutput;
import contrail.PopBubbles;
import contrail.PopBubblesMessage;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

/**
 * Popbubbles is the continued phase of removing bubbles from graph. 
 * Consider the graph X->{A,B}->Y 
 * FindBubbles gathers information about the potential pubbles at the major node (in this case Y). 
 * The node with * higher coverage (A) is preserved and the edge from the major node to the node to
 * be popped (B) is removed.
 * hence the resultant graph is reduced to X->A->Y and X->B
 * we now want to reduce it to X->A->Y
 * We know that in earlier stage node B was marked to be removed as its sequence was similar to A
 * it was disconnected from Y; but not from X; Hence B is the dead node.
 * 
 * Mapper: The basic function of mapper is to send 4 types of messages to reducer of PopBubbles
 *  --It takes input of graph_data and then extracts the BubbleInfo that was populated in FindBubbles Reducer stage 
 *  Mapper sends 4 types of messages:
 *  Kill link (L), Kill Message (M), Extra Coverage (C), Normal (N)
 *  to disconnect B from X we send Reducer nodes according to their minor Nodes; {minor(A)== minor(B)= node X}
 *  
 * Reducer: based on the type of message it receives from the Mapper; Reducer performs the following function
 *  N: Normal; set sawnode flag and set Node
 *  C: update the coverage of the node after popping bubbles
 *  M: this sets the kill node flag; it'll increment the counter that keeps track of the bubbles removed
 *  L: this causes Reducer to disconnect dead node from Minor node (B is disconnected from X)
 */
public class PopBubblesAvro extends Stage  {

  private static final Logger sLogger = Logger.getLogger(PopBubblesAvro.class);

  public static final Schema MAP_OUT_SCHEMA = 
      Pair.getPairSchema(Schema.create(Schema.Type.STRING), 
          (new PopBubblesMessage()).getSchema());

  private static Pair<CharSequence, PopBubblesMessage> out_pair = 
      new Pair<CharSequence, PopBubblesMessage>(MAP_OUT_SCHEMA);

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return defs;
  }
  // PopBubblesMapper
  ///////////////////////////////////////////////////////////////////////////
  public static class PopBubblesAvroMapper 
  extends AvroMapper<FindBubblesOutput, Pair<CharSequence,PopBubblesMessage>>   {

    PopBubblesMessage msg = null;
    GraphNode node= null;

    public void configure(JobConf job)   {
      node= new GraphNode();
      msg= new PopBubblesMessage();
    }
    public void map(FindBubblesOutput reducer_msg,
        AvroCollector<Pair<CharSequence, PopBubblesMessage>> output, 
        Reporter reporter) throws IOException   {

      node.setData(reducer_msg.getNode());
      List<contrail.BubbleInfo> bubbles = reducer_msg.getNodeBubbleinfo();  // we saved this list in FindBubble

      if (bubbles != null)    {
        for(contrail.BubbleInfo bubble : bubbles)  {
          {
            //Message:  Kill Link
            msg.setNodeMessage("L");		
            msg.setDeadNodeID(bubble.getNodetoRemoveID());
           
            msg.setAliveNodeID(bubble.getAliveNodeID());
            msg.setNewCoverage((float) 0);
            msg.setNode(null);

            out_pair.set(bubble.getTargetID(), msg);
            output.collect(out_pair);
          }
          /*{ 
            // Message:  Kill Msg
            msg.setNodeMessage("M");
            msg.setAliveNodeID(null);
            msg.setDeadNode(null);
            msg.setNewCoverage(null);
            msg.setNode(null);

            out_pair.set(bubble.getNodetoRemove().getNodeId(), msg);
            output.collect(out_pair);
          }*/
          // TODO: i guess there is no need to send msg to bubble node 
          {
            // Message: EXTRACOV
            msg.setNodeMessage("C");		
            msg.setNewCoverage(bubble.getExtraCoverage());
            msg.setAliveNodeID("");
            msg.setDeadNodeID("");
            msg.setNode(null);
            
            out_pair.set(bubble.getAliveNodeID(), msg);
            output.collect(out_pair);
          }
          reporter.incrCounter("Contrail", "bubblespopped", 1);
        }
      }
      // Message: normal node
      msg.setNodeMessage("N");		
      msg.setNode(node.getData());
      msg.setAliveNodeID("");
      msg.setDeadNodeID("");
      msg.setNewCoverage((float) 0);
      
      out_pair.set(node.getNodeId(), msg);
      output.collect(out_pair);

      reporter.incrCounter("Contrail", "nodes", 1);
    }
  }

  // PopBubblesReducer
  ///////////////////////////////////////////////////////////////////////////
  public static class PopBubblesAvroReducer 
  extends AvroReducer<CharSequence, PopBubblesMessage, Pair<CharSequence, GraphNodeData>>	
  {
    private static int K = 0;
    GraphNode node = null;
    PopBubblesMessage msg= null;

    public void configure(JobConf job) {
      PopBubblesAvro stage= new PopBubblesAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));

      node= new GraphNode();
      msg= new PopBubblesMessage();
    }

    public void reduce(CharSequence nodeid, Iterable<PopBubblesMessage> iterable,
        AvroCollector<GraphNodeData> output, Reporter reporter)
            throws IOException  
            {
      int sawnode = 0;
      Iterator<PopBubblesMessage> iter = iterable.iterator();
      ArrayList<String>Nodes_to_Remove = new ArrayList<String>();

      //boolean killnode = false;
      float extracov = 0;
      
      while(iter.hasNext())
      {
        msg = iter.next();
        PopBubblesMessage copy= new PopBubblesMessage();

        copy.setNode(msg.getNode());
        copy.setNewCoverage(msg.getNewCoverage());
        copy.setAliveNodeID(msg.getAliveNodeID());
        copy.setDeadNodeID(msg.getDeadNodeID());
        copy.setNodeMessage(msg.getNodeMessage());
        CharSequence Node_msg= copy.getNodeMessage();

        if(Node_msg.equals("N"))
        {
          node.setData(msg.getNode());
          node= node.clone();
          sawnode++;
        }
        else if (Node_msg.equals("L"))
        {
          String node_to_remove = (String) copy.getDeadNodeID();
          Nodes_to_Remove.add(node_to_remove);
        }
        /*else if (Node_msg.equals("M"))
        {
          killnode = true;
        }*/
        else if (Node_msg.equals("C"))
        {
          extracov += copy.getNewCoverage();
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
      /*
      if (killnode)
      {
        reporter.incrCounter("Contrail", "bubblenodes_removed", 1);
        return;
      }*/

      if (extracov > 0)
      {
        int merlen = node.getData().getSequence().getLength() - K + 1;
        float support = node.getCoverage() * merlen + extracov;
        node.setCoverage((float) support /  (float) merlen);
      }

      if (Nodes_to_Remove.size() > 0)
      {
        for(String node_to_remove : Nodes_to_Remove)
        {
          node.removeNeighbor(node_to_remove);
          reporter.incrCounter("Contrail", "linksremoved", 1);
        }
      }
      output.collect(node.getData());
    }
  }

  // Run Tool
  ///////////////////////////////////////////////////////////////////////////	

  protected int run() throws Exception
  {

    String[] required_args = {"inputpath", "outputpath", "K"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    int K = (Integer)stage_options.get("K");

    sLogger.info("Tool name: PopBubbles");
    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } 
    else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("PopBubbles " + inputPath + " " + K);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graph_data = new GraphNodeData();
    AvroJob.setInputSchema(conf, new FindBubblesOutput().getSchema());  
    AvroJob.setMapOutputSchema(conf, PopBubblesAvro.MAP_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, PopBubblesAvroMapper.class);
    AvroJob.setReducerClass(conf, PopBubblesAvroReducer.class);

    AvroJob.setOutputSchema(conf, graph_data.getSchema());

    //delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    JobClient.runJob(conf);
    return 0;
  }

  // Main
  ///////////////////////////////////////////////////////////////////////////	

  public static void main(String[] args) throws Exception 
  {
    int res = ToolRunner.run(new Configuration(), new PopBubbles(), args);
    System.exit(res);
  }
}
