/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.tools;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
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

import contrail.graph.GraphNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * Selects and outputs the compressible subgraph.
 *
 * Before running this binary, you must first mark the compressible nodes
 * by running {@link contrail.stages.CompressibleAvro}. You must then
 * run {@link contrail.stages.QuickMarkAvro} in order to identify any nodes
 * connected to the compressible nodes.
 *
 * This code could also be used to select all nodes with a given read tag.
 *
 * The output of this mapreduce is the set of nodes that would be sent to
 * QuickMerge to compress all nodes in one shot. This tool is primarily
 * intended for debugging.
 */
public class SelectCompressibleSubgraph  extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      SelectCompressibleSubgraph.class);

  public static class SubgraphMapper extends
      AvroMapper<GraphNodeData, GraphNodeData> {

    String targetTag;

    public void Configure(JobConf job) {
      SelectCompressibleSubgraph stage = new SelectCompressibleSubgraph();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      targetTag = (String)(definitions.get("read_tag").parseJobConf(job));
    }

    @Override
    public void map(
        GraphNodeData graphData,
        AvroCollector<GraphNodeData> collector,
        Reporter reporter) throws IOException {
      String readTag = graphData.getMertag().getReadTag().toString();

      if (!readTag.equals(targetTag)) {
        return;
      }

      collector.collect(graphData);
      reporter.incrCounter("Contrail", "nodes", 1);
    }
  }

  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition readTag = new ParameterDefinition(
        "read_tag",  "The value of the read tag to select." , String.class,
        "compress");

    defs.put(readTag.getName(), readTag);
    return defs;
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);


    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf == null) {
      conf = new JobConf(SelectCompressibleSubgraph.class);
    } else {
      conf = new JobConf(base_conf, SelectCompressibleSubgraph.class);
    }
    this.setConf(conf);
    conf.setJobName("SelectCompressibleSubgraph " + inputPath);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graphData = new GraphNodeData();
    AvroJob.setInputSchema(conf, graphData.getSchema());
    AvroJob.setMapOutputSchema(conf, graphData.getSchema());

    // Job is mapper only.
    conf.setNumReduceTasks(0);

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
      RunningJob result = JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();
      float diff = (float) ((endtime - starttime) / 1000.0);

      sLogger.info("Runtime: " + diff + " s");
      return result;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new SelectCompressibleSubgraph(), args);
    System.exit(res);
  }
}

