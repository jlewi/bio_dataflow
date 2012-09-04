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

package contrail.stages;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.util.FileHelper;

/**
 * This stage iteratively compresses chains and does error correction.
 *
 * Each round of compression can expose new topological errors that can be
 * corrected. Similarly, correcting errors can expose new chains which can
 * be compressed. Consequently, we repeatedly perform compression followed
 * by error correction until we have a round where the graph doesn't change.
 */
public class CompressAndCorrect extends Stage {
  private static final Logger sLogger = Logger.getLogger(CompressChains.class);
  /**
   * Get the parameters used by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    // We add all the options for the stages we depend on.
    Stage[] substages =
      {new CompressChains(), new RemoveTipsAvro(), new FindBubblesAvro(),
       new PopBubblesAvro(), new RemoveLowCoverageAvro()};

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * This class is used to return information about the sub jobs that are run.
   */
  protected static class JobInfo {
    // True if the graph changed.
    public boolean graphChanged;
    // The path to the graph.
    public String graphPath;
    // A message summarizing what happened.
    public String logMessage;
  }

  private JobInfo compressGraph(String inputPath, String outputPath)
      throws Exception {
    CompressChains compressStage = new CompressChains();
    compressStage.setConf(getConf());
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            compressStage.getParameterDefinitions().values());

    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    compressStage.setParameters(stageOptions);
    RunningJob compressJob = compressStage.runJob();

    // TODO(jlewi): We should compute how much the graph changed and output
    // this as part of the log message.
    JobInfo result = new JobInfo();
    result.logMessage = "CompressChains ran.";
    result.graphPath = compressStage.getFinalGraphPath();
    return result;
  }

  /**
   * Remove the tips in the graph.
   * @param inputPath
   * @param outputPath
   * @return True if any tips were removed.
   */
  private JobInfo removeTips(String inputPath, String outputPath)
      throws Exception {
    RemoveTipsAvro stage = new RemoveTipsAvro();
    stage.setConf(getConf());
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            stage.getParameterDefinitions().values());

    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);
    RunningJob job = stage.runJob();

    JobInfo result = new JobInfo();
    if (job == null) {
      result.logMessage =
          "RemoveTips stage was skipped because graph would not change.";
      sLogger.info(result.logMessage);
      result.graphPath = inputPath;
      result.graphChanged = false;
      return result;
    }

    // Check if any tips were found.
    long tipsRemoved = job.getCounters().findCounter(
        RemoveTipsAvro.NUM_REMOVED.group,
        RemoveTipsAvro.NUM_REMOVED.tag).getValue();

    if (tipsRemoved > 0) {
      result.graphChanged = true;
    }
    result.graphPath = outputPath;

    Formatter formatter = new Formatter(new StringBuilder());
    result.logMessage = formatter.format(
        "RemoveTips: number of nodes removed %d", tipsRemoved).toString();
    return result;
  }

  /**
   * PopBubbles in the graph.
   * @param inputPath
   * @param outputPath
   * @return JobInfo
   */
  private JobInfo popBubbles(String inputPath, String outputPath)
      throws Exception {
    FindBubblesAvro findStage = new FindBubblesAvro();
    findStage.setConf(getConf());
    String findOutputPath = new Path(outputPath, "FindBubbles").toString();
    {
      // Make a shallow copy of the stage options required.
      Map<String, Object> stageOptions =
          ContrailParameters.extractParameters(
              this.stage_options,
              findStage.getParameterDefinitions().values());

      stageOptions.put("inputpath", inputPath);
      stageOptions.put("outputpath", findOutputPath);
      findStage.setParameters(stageOptions);
    }

    RunningJob findJob = findStage.runJob();

    if (findJob == null) {
      JobInfo result = new JobInfo();
      result.logMessage =
          "FindBubbles stage was skipped because graph would not change.";
      sLogger.info(result.logMessage);
      result.graphPath = inputPath;
      result.graphChanged = false;
      return result;
    }

    // Check if any bubbles were found.
    long bubblesFound = findJob.getCounters().findCounter(
        FindBubblesAvro.num_bubbles.group,
        FindBubblesAvro.num_bubbles.tag).getValue();

    if (bubblesFound == 0) {
      // Since no bubbles were found, we don't need to run the second phase
      // of pop bubbles.
      JobInfo result = new JobInfo();
      result.graphChanged = false;
      // Since the graph didn't change return the input path as the path to
      // the graph.
      result.graphPath = inputPath;
      result.logMessage =
          "FindBubbles found 0 bubbles.";
      return result;
    }

    PopBubblesAvro popStage = new PopBubblesAvro();
    popStage.setConf(getConf());
    String popOutputPath = new Path(outputPath, "PopBubbles").toString();
    {
      // Make a shallow copy of the stage options required.
      Map<String, Object> stageOptions =
          ContrailParameters.extractParameters(
              this.stage_options,
              popStage.getParameterDefinitions().values());

      stageOptions.put("inputpath", findOutputPath);
      stageOptions.put("outputpath", popOutputPath);
      popStage.setParameters(stageOptions);
    }
    RunningJob popJob = popStage.runJob();

    JobInfo result = new JobInfo();
    result.graphChanged = true;
    result.graphPath = popOutputPath;

    Formatter formatter = new Formatter(new StringBuilder());
    result.logMessage = formatter.format(
        "FindBubbles: number of nodes removed %d", bubblesFound).toString();
    return result;
  }

  /**
   * Remove low coverage nodes.
   * @param inputPath
   * @param outputPath
   * @return True if any tips were removed.
   */
  private JobInfo removeLowCoverageNodes(String inputPath, String outputPath)
      throws Exception {
    RemoveLowCoverageAvro stage = new RemoveLowCoverageAvro();
    stage.setConf(getConf());
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            stage.getParameterDefinitions().values());

    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);
    RunningJob job = stage.runJob();

    JobInfo result = new JobInfo();
    if (job == null) {
      result.logMessage =
          "RemoveLowCoverage stage was skipped because graph would not " +
           "change.";
      sLogger.info(result.logMessage);
      result.graphPath = inputPath;
      result.graphChanged = false;
      return result;
    }
    // Check if any tips were found.
    long nodesRemoved = job.getCounters().findCounter(
        RemoveLowCoverageAvro.NUM_REMOVED.group,
        RemoveLowCoverageAvro.NUM_REMOVED.tag).getValue();

    result.graphPath = outputPath;
    if (nodesRemoved > 0) {
      result.graphChanged = true;
    }

    Formatter formatter = new Formatter(new StringBuilder());
    result.logMessage = formatter.format(
        "RemoveLowCoverage removed %d nodes.", nodesRemoved).toString();
    return result;
  }

  private void processGraph() throws Exception {
    // TODO(jlewi): Does this function really need to throw an exception?
    String outputPath = (String) stage_options.get("outputpath");

    // Create a subdirectory of the output path to contain the temporary
    // output from each substage.
    String tempPath = new Path(outputPath, "temp").toString();
    int step = 0;

    // A list of log messages that summarizes what happened at each stage.
    // We print this out at the end of the input for convenience.
    ArrayList<String> logMessages = new ArrayList<String>();
    logMessages.add("A summary of the various processing that happened.");

    // When formatting the step as a string we want to zero pad it
    DecimalFormat sf = new DecimalFormat("00");

    // Keep track of the latest input for the step.
    String stepInputPath = (String) stage_options.get("inputpath");
    boolean  done = false;

    while (!done) {
      ++step;
      logMessages.add("Step " + sf.format(step).toString());

      // Create a subdirectory of the temp directory to contain the output
      // from this round.
      String stepPath = new Path(
          tempPath, "step_" +sf.format(step)).toString();

      // We only remove low coverage nodes on the first step because the
      // subsequent stages shouldn't decrease coverage for any nodes.
      if (step == 1) {
        String lowCoveragePath =
            new Path(stepPath, "LowCoveragePath").toString();
        JobInfo result =
            removeLowCoverageNodes(stepInputPath, lowCoveragePath);
        logMessages.add(result.logMessage);
        stepInputPath = result.graphPath;
      }

      // Paths to use for this round. The paths for the compressed graph
      // and the error corrected graph.
      String compressedPath = new Path(
          stepPath, "CompressChains").toString();
      String removeTipsPath = new Path(stepPath, "RemoveTips").toString();

      JobInfo compressResult = compressGraph(stepInputPath, compressedPath);
      logMessages.add(compressResult.logMessage);

      JobInfo tipsResult = removeTips(compressResult.graphPath, removeTipsPath);
      logMessages.add(tipsResult.logMessage);

      if (tipsResult.graphChanged) {
        stepInputPath = tipsResult.graphPath;
        // We need to recompress the graph before continuing.
        continue;
      }

      // There were no tips, so the graph is maximally compressed. Try
      // finding and removing bubbles.
      String popBubblesPath = new Path(stepPath, "PoppedBubbles").toString();
      JobInfo popResult = popBubbles(tipsResult.graphPath, popBubblesPath);
      logMessages.add(popResult.logMessage);
      stepInputPath = popResult.graphPath;
      if (!popResult.graphChanged) {
        done = true;
      }
    }


    sLogger.info("Save result to: " + outputPath + "\n\n");
    FileHelper.moveDirectoryContents(getConf(), stepInputPath, outputPath);
    logMessages.add("Final graph saved to:" + outputPath);

    // Clean up the intermediary directories.
    // TODO(jlewi): We might want to add an option to keep the intermediate
    // directories.
    sLogger.info("Delete temporary directory: " + tempPath + "\n\n");
    FileSystem.get(getConf()).delete(new Path(tempPath), true);

    // Print out the summary of what happened.
    String summary = StringUtils.join(logMessages, "\n");
    sLogger.info(summary);
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    if (stage_options.containsKey("writeconfig")) {
      // TODO(jlewi): Can we write the configuration for this stage like
      // other stages or do we need to do something special?
      throw new NotImplementedException(
          "Support for writeconfig isn't implemented yet for " +
          "CompressAndCorrect");
    } else {
      long starttime = System.currentTimeMillis();
      processGraph();
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);
      sLogger.info("Runtime: " + diff + " s");
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CompressAndCorrect(), args);
    System.exit(res);
  }
}
