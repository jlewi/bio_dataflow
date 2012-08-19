package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Formatter;
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
 *
 * In the first phase, FindBubbles, bubbles were identified and the node with
 * lower coverage was deleted. In this stage, we update nodes which had edges
 * to the deleted nodes.
 *
 * The mapper simply routes the messages outputted by FindBubbles to the
 * appropriate target.
 *
 * The reducer applies the delete edge messages to nodes and outputs the graph.
 */
public class PopBubblesAvro extends Stage  {
  private static final Logger sLogger = Logger.getLogger(PopBubblesAvro.class);

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

  public static class PopBubblesAvroMapper
    extends AvroMapper<FindBubblesOutput,
                       Pair<CharSequence, FindBubblesOutput>> {
    Pair<CharSequence, FindBubblesOutput> outPair = null;

    public void configure(JobConf job)   {
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

  public static class PopBubblesAvroReducer
    extends AvroReducer<CharSequence, FindBubblesOutput, GraphNodeData> {
    GraphNode node = null;
    ArrayList<String> neighborsToRemove;

    public void configure(JobConf job) {
      PopBubblesAvro stage = new PopBubblesAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();

      node = new GraphNode();
      neighborsToRemove = new ArrayList<String>();
    }

    public void reduce(
        CharSequence nodeid, Iterable<FindBubblesOutput> iterable,
        AvroCollector<GraphNodeData> output, Reporter reporter)
            throws IOException {
      int sawNode = 0;
      Iterator<FindBubblesOutput> iter = iterable.iterator();
      neighborsToRemove.clear();

      while(iter.hasNext()) {
        FindBubblesOutput input = iter.next();

        if (input.getNode() != null) {
          node.setData(input.getNode());
          node = node.clone();
          sawNode++;
        } else {
          for (CharSequence  neighbor : input.getDeletedNeighbors()) {
            neighborsToRemove.add(neighbor.toString());
          }
        }
      }

      if (sawNode == 0)    {
        Formatter formatter = new Formatter(new StringBuilder());
        formatter.format(
            "ERROR: No node was provided for nodeId %s. This can happen if " +
            "the graph isn't maximally compressed before calling FindBubbles",
            nodeid);
        throw new IOException(formatter.toString());
      }

      if (sawNode > 1) {
        Formatter formatter = new Formatter(new StringBuilder());
        formatter.format("ERROR: nodeId %s, %d nodes were provided",
            nodeid, sawNode);
        throw new IOException(formatter.toString());
      }

      for(String neighborID : neighborsToRemove) {
        node.removeNeighbor(neighborID);
        reporter.incrCounter("Contrail", "linksremoved", 1);
      }
      output.collect(node.getData());
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

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
    conf.setJobName("PopBubbles " + inputPath);

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
