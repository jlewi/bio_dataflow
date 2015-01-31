/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;

/**
 * For each threadable node this stage outputs a group of ids consisting
 * of the threadable node and its neighbors. In subsequent steps
 * we group those nodes together and resolve the threads.
 */
public class SplitThreadableGraph extends MRStage {
  private static final Logger sLogger = Logger.getLogger(
      SplitThreadableGraph.class);
  private static String THREADABLE_COUNTER = "num-threadable";

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(defs);
  }

  public static class Mapper extends
      AvroMapper<GraphNodeData, List<CharSequence>> {
    private SortedSet<String> outIds;
    private ArrayList<CharSequence> outIdsList;
    private GraphNode node;

    @Override
    public void configure(JobConf job) {
      outIds = new TreeSet<String>();
      outIdsList = new ArrayList<CharSequence>();
      node = new GraphNode();
    }

    @Override
    public void map(
        GraphNodeData nodeData, AvroCollector<List<CharSequence>> collector,
        Reporter reporter)
            throws IOException {
      outIds.clear();
      outIdsList.clear();
      node.setData(nodeData);
      outIds.add(node.getNodeId());

      if (node.getNeighborIds().size() == 0) {
        reporter.getCounter("contrail", "not-threadable").increment(1);
        reporter.getCounter("contrail", "islands").increment(1);
        return;
      }

      NodeThreadInfo threadInfo = new NodeThreadInfo(node);
      if (!threadInfo.isThreadable()) {
        // Don't output any information for unthreadable nodes.
        reporter.getCounter("contrail", "not-threadable").increment(1);
        return;
      }

      // We use a set to sort the ids and ensure each node appears at most
      // once.
      outIds.addAll(node.getNeighborIds());

      // Convert it to a list.
      outIdsList.addAll(outIds);

      reporter.getCounter("contrail", THREADABLE_COUNTER).increment(1);
      collector.collect(outIdsList);
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroJob.setInputSchema(conf, new GraphNodeData().getSchema());
    AvroJob.setMapOutputSchema(
        conf, Schema.createArray(Schema.create(Schema.Type.STRING)));

    AvroJob.setOutputSchema(
        conf, Schema.createArray(Schema.create(Schema.Type.STRING)));

    AvroJob.setMapperClass(conf, Mapper.class);
    // This is a mapper only job.
    conf.setNumReduceTasks(0);
  }

   /**
   * Returns the number of nodes in the graph which are threadable.
   */
  public long getNumThreadable() {
    return getCounter("contrail", THREADABLE_COUNTER);
  }

  public static void main(String[] args) throws Exception {
    SplitThreadableGraph stage = new SplitThreadableGraph();
    int res = stage.run(args);
    System.exit(res);
  }
}
