package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

/**
 * Popbubbles is the second phase of removing bubbles from the graph.
 * Consider the graph X->{A,B}->Y
 * FindBubbles gathers information about the potential bubbles at the major node
 * (in this case Y).
 * The node with higher coverage (A) is preserved and the edge from the major node to the node to
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

//  public static final Schema MAP_OUT_SCHEMA =
//      Pair.getPairSchema(Schema.create(Schema.Type.STRING),
//          (new PopBubblesMessage()).getSchema());
//
//  private static Pair<CharSequence, PopBubblesMessage> out_pair =
//      new Pair<CharSequence, PopBubblesMessage>(MAP_OUT_SCHEMA);

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
    extends AvroMapper<FindBubblesOutput, Pair<CharSequence, FindBubblesOutput>> {
    Pair<CharSequence, FindBubblesOutput> outPair = null;
    GraphNode node= null;

    public void configure(JobConf job)   {
      node= new GraphNode();
      outPair = new Pair<CharSequence, FindBubblesOutput> (
          "", new FindBubblesOutput());
    }

    public void map(FindBubblesOutput input,
        AvroCollector<Pair<CharSequence, FindBubblesOutput>> collector,
        Reporter reporter) throws IOException {
      if (input.getNode() != null) {
        outPair.key(input.getNode().getNodeId());
      } else {
        outPair.key(input.getMinorNodeId());
      }
      outPair.value(input);
      collector.collect(outPair);
    }
  }

  // PopBubblesReducer
  ///////////////////////////////////////////////////////////////////////////
  public static class PopBubblesAvroReducer
    extends AvroReducer<CharSequence, FindBubblesOutput, GraphNodeData>
  {
    private static int K = 0;
    GraphNode node = null;
    // FindBubblesOutput msg= null;
    //GraphNodeData outputNode
    ArrayList<String> idsToRemove;

    public void configure(JobConf job) {
      PopBubblesAvro stage = new PopBubblesAvro();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));

      node= new GraphNode();
      //msg= new PopBubblesMessage();
      idsToRemove = new ArrayList<String>();
    }

    public void reduce(CharSequence nodeid, Iterable<FindBubblesOutput> iterable,
        AvroCollector<GraphNodeData> output, Reporter reporter)
            throws IOException
            {
      int sawnode = 0;
      Iterator<FindBubblesOutput> iter = iterable.iterator();
      //ArrayList<String> Nodes_to_Remove = new ArrayList<String>();
      idsToRemove.clear();

      //boolean killnode = false;
      float extracov = 0;

      while(iter.hasNext())
      {
        FindBubblesOutput input = iter.next();

        if (input.getNode() != null) {
          node.setData(input.getNode());
          node = node.clone();
          sawnode++;
        } else {
          for (BubbleMinorMessage  bubbleInfo : input.getMinorMessages()) {
            idsToRemove.add(bubbleInfo.getNodetoRemoveID().toString());
            extracov += bubbleInfo.getExtraCoverage();
          }
        }
//        PopBubblesMessage copy= new PopBubblesMessage();
//
//        copy.setNode(msg.getNode());
//        copy.setNewCoverage(msg.getNewCoverage());
//        copy.setAliveNodeID(msg.getAliveNodeID());
//        copy.setDeadNodeID(msg.getDeadNodeID());
//        copy.setNodeMessage(msg.getNodeMessage());
//        CharSequence Node_msg= copy.getNodeMessage();
//
//        if(Node_msg.equals("N"))
//        {
//          node.setData(msg.getNode());
//          node= node.clone();
//          sawnode++;
//        }
//        else if (Node_msg.equals("L"))
//        {
//          String node_to_remove = (String) copy.getDeadNodeID();
//          Nodes_to_Remove.add(node_to_remove);
//        }
//        /*else if (Node_msg.equals("M"))
//        {
//          killnode = true;
//        }*/
//        else if (Node_msg.equals("C"))
//        {
//          extracov += copy.getNewCoverage();
//        }
//        else
//        {
//          throw new IOException("Unknown msgtype: " + msg);
//        }
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

      if (extracov > 0) {
        // We increase the coverage based on the edges the edges to the deleted
        // nodes because those edges are assumed to  result from errors.
        int merlen = node.getData().getSequence().getLength() - K + 1;
        float support = node.getCoverage() * merlen + extracov;
        node.setCoverage(support /  merlen);
      }

      for(String neighborID : idsToRemove) {
        node.removeNeighbor(neighborID);
        reporter.incrCounter("Contrail", "linksremoved", 1);
      }
      output.collect(node.getData());
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
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

    GraphNodeData graphData = new GraphNodeData();
    AvroJob.setInputSchema(conf, new FindBubblesOutput().getSchema());

    Pair<CharSequence, FindBubblesOutput> mapOutput =
        new Pair<CharSequence, FindBubblesOutput> ("", new FindBubblesOutput());

    AvroJob.setMapOutputSchema(conf, mapOutput.getSchema());

    AvroJob.setMapperClass(conf, PopBubblesAvroMapper.class);
    AvroJob.setReducerClass(conf, PopBubblesAvroReducer.class);

    AvroJob.setOutputSchema(conf, graphData.getSchema());

    //delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    RunningJob job = JobClient.runJob(conf);
    return job;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PopBubblesAvro(), args);
    System.exit(res);
  }
}
