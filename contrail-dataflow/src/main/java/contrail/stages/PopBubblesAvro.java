/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
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
public class PopBubblesAvro extends MRStage  {
  private static final Logger sLogger = Logger.getLogger(PopBubblesAvro.class);

  @Override
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

    @Override
    public void configure(JobConf job)   {
      outPair = new Pair<CharSequence, FindBubblesOutput> (
          "", new FindBubblesOutput());
    }

    @Override
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

    @Override
    public void configure(JobConf job) {
      PopBubblesAvro stage = new PopBubblesAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();

      node = new GraphNode();
      neighborsToRemove = new ArrayList<String>();
    }

    @Override
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
  protected void setupConfHook() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    JobConf conf = (JobConf) getConf();
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
  }

  @Override
  protected void postRunHook() {
    try {
      long numNodes = job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "REDUCE_OUTPUT_RECORDS").getValue();
      sLogger.info("Number of nodes outputed:" + numNodes);
    } catch (IOException e) {
      sLogger.fatal("Couldn't get counters.", e);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PopBubblesAvro(), args);
    System.exit(res);
  }
}
