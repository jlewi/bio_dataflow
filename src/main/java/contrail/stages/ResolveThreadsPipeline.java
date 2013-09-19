/*
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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import contrail.util.ContrailLogger;
import contrail.util.FileHelper;

/**
 * Pipeline for resolving all the threads in a graph.
 */
public class ResolveThreadsPipeline extends PipelineStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(ResolveThreadsPipeline.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    // We add all the options for the stages we depend on.
    StageBase[] substages =
      {new SplitThreadableGraph(), new SelectThreadableGroups(),
       new ResolveThreads()};

    for (StageBase stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    ParameterDefinition stats = ContrailParameters.getComputeStats();
    definitions.put(stats.getName(), stats);

    ParameterDefinition cleanup = ContrailParameters.getCleanup();
    definitions.put(cleanup.getName(), cleanup);

    return Collections.unmodifiableMap(definitions);
  }

  @Override
  protected void stageMain() {
    int step = -1;
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    String lastOutput = null;
    while (true) {
      ++step;
      sLogger.info(String.format("Starting step %d", step));

      String stepDir = FilenameUtils.concat(
          outputPath, String.format("step_%03d", step));

      // Split the graph into subgraphs for which we will resolve the threads.
      SplitThreadableGraph splitStage = new SplitThreadableGraph();
      splitStage.initializeAsChild(this);
      String splitOutput = FilenameUtils.concat(
          stepDir, splitStage.getClass().getSimpleName());
      splitStage.setParameter("inputpath", inputPath);
      splitStage.setParameter("outputpath", splitOutput);

      if (!executeChild(splitStage)) {
        sLogger.fatal(
            "SplitStage failed.", new RuntimeException("Stage failure."));
      }

      if (splitStage.getNumThreadable() == 0) {
        // No threadable nodes.
        sLogger.info("No nodes with threads to resolve.");
        break;
      }

      // Select the groups resolve.
      SelectThreadableGroups selectStage = new SelectThreadableGroups();
      selectStage.initializeAsChild(this);
      selectStage.setParameter(
          "inputpath",
          FilenameUtils.concat(splitOutput, "*avro"));

      String selectOutput = FilenameUtils.concat(
          stepDir, selectStage.getClass().getSimpleName());

      selectStage.setParameter("outputpath", selectOutput);

      if (!executeChild(selectStage)) {
        sLogger.fatal(
            "SelectThreadableGroups stage failed.",
            new RuntimeException("Stage failure."));
      }

      // Rekey the nodes by component id.
      // Nodes which aren't threadable or neighbors of threadable nodes will
      // just be keyed by their node Id.
      RekeyByComponentId rekeyStage = new RekeyByComponentId();
      rekeyStage.initializeAsChild(this);
      List<String> rekeyInputs = Arrays.asList(
          selectStage.getOutPath().toString(), inputPath);
      rekeyStage.setParameter("inputpath", StringUtils.join(rekeyInputs, ","));

      String rekeyOutput = FilenameUtils.concat(
          stepDir, rekeyStage.getClass().getSimpleName());
      rekeyStage.setParameter("outputpath", rekeyOutput);

      if (!executeChild(rekeyStage)) {
        sLogger.fatal(
            "Rekey stage failed.", new RuntimeException("Stage failure."));
      }

      // Group the nodes by component.
      GroupByComponentId groupStage = new GroupByComponentId();
      groupStage.initializeAsChild(this);
      groupStage.setParameter("inputpath", rekeyOutput);

      String groupOutput = FilenameUtils.concat(
          stepDir, groupStage.getClass().getSimpleName());

      groupStage.setParameter("outputpath", groupOutput);

      if (!executeChild(groupStage)) {
        sLogger.fatal(
            "GroupBy stage failed.", new RuntimeException("Stage failure."));
      }

      // Resolve the threads.
      ResolveThreads resolveStage = new ResolveThreads();
      resolveStage.initializeAsChild(this);
      resolveStage.setParameter("inputpath", groupOutput);

      String resolveOutput = FilenameUtils.concat(
          stepDir, resolveStage.getClass().getSimpleName());
      resolveStage.setParameter("outputpath", resolveOutput);

      if (!executeChild(resolveStage)) {
        sLogger.fatal(
            "GroupBy stage failed.", new RuntimeException("Stage failure."));
      }
      lastOutput = resolveOutput;
      inputPath = resolveOutput;
    }

    if (step > 0) {
      sLogger.info("Save result to: " + outputPath + "\n\n");
      FileHelper.moveDirectoryContents(getConf(), lastOutput, outputPath);
      sLogger.info("Final graph saved to:" + outputPath);

      // Record the fact that for the last substage we moved its output.
      StageInfo lastInfo =
          stageInfo.getSubStages().get(stageInfo.getSubStages().size() - 1);

      StageParameter finalPathParameter = new StageParameter();
      finalPathParameter.setName("outputpath");
      finalPathParameter.setValue(outputPath);
      lastInfo.getModifiedParameters().add(finalPathParameter);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new ResolveThreadsPipeline(), args);
    System.exit(res);
  }
}
