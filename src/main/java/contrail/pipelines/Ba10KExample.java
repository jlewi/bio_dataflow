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

package contrail.pipelines;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import contrail.stages.BuildGraphAvro;
import contrail.stages.CompressAndCorrect;
import contrail.stages.ContrailParameters;
import contrail.stages.FastqPreprocessorAvroCompressed;
import contrail.stages.GraphStats;
import contrail.stages.NotImplementedException;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * An example which runs all stages required to process the Ba10K dataset.
 *
 */
public class Ba10KExample extends Stage {
  private static final Logger sLogger = Logger.getLogger(Ba10KExample.class);

  public Ba10KExample() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }
  /**
   * Get the parameters used by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    // We add all the options for the stages we depend on.
    Stage[] substages =
      {new FastqPreprocessorAvroCompressed(), new BuildGraphAvro(),
       new CompressAndCorrect(), new GraphStats()};

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * Set the default parameters for the ba10K dataset.
   */
  protected void setDefaultParameters() {
    // Set any parameters to the default value if they haven't already been
    // set.
    if (!stage_options.containsKey("K")) {
      stage_options.put("K", new Integer(21));
    }
    if (!stage_options.containsKey("tiplength")) {
      stage_options.put("tiplength", new Integer(42));
    }
    if (!stage_options.containsKey("bubble_edit_rate")) {
      stage_options.put("bubble_edit_rate", new Float(.05f));
    }
    if (!stage_options.containsKey("bubble_length_threshold")) {
      stage_options.put("bubble_length_threshold", new Integer(42));
    }
    if (!stage_options.containsKey("length_thresh")) {
      stage_options.put("length_thresh", new Integer(42));
    }
    if (!stage_options.containsKey("low_cov_thresh")) {
      stage_options.put("low_cov_thresh", new Float(5.0f));
    }
  }

  private void processGraph() throws Exception {
    // TODO(jlewi): Does this function really need to throw an exception?
    String outputPath = (String) stage_options.get("outputpath");

    Stage[] subStages =
      {new FastqPreprocessorAvroCompressed(), new BuildGraphAvro(),
       new CompressAndCorrect()};


    // The path to process for the next stage.
    String latestPath = (String) stage_options.get("inputpath");

    for (Stage stage : subStages) {
      // Make a shallow copy of the stage options required by the stage.
      Map<String, Object> stageOptions =
          ContrailParameters.extractParameters(
              this.stage_options,
              stage.getParameterDefinitions().values());

      stage.setConf(getConf());

      String stageOutput =
          new Path(outputPath, stage.getClass().getName()).toString();
      stageOptions.put("inputpath", latestPath);
      stageOptions.put("outputpath", stageOutput);

      stage.setParameters(stageOptions);
      RunningJob job = stage.runJob();
      if (job !=null && !job.isSuccessful()) {
        throw new RuntimeException(
            String.format(
                "Stage %s had a problem", stage.getClass().getName()));
      }

      latestPath = stageOutput;
      // We compute graph stats for each stage except the conversion
      // of Fastq files to Avro.
      if (FastqPreprocessorAvroCompressed.class.isInstance(stage)) {
        continue;
      }
      GraphStats statsStage = new GraphStats();
      statsStage.setConf(getConf());
      String statsOutput = new Path(
          outputPath,
          String.format("%sStats", stage.getClass().getName())).toString();
      Map<String, Object> statsParameters = new HashMap<String, Object>();
      statsParameters.put("inputpath", stageOutput);
      statsParameters.put("outputpath", statsOutput);
      statsStage.setParameters(statsParameters);

      RunningJob statsJob = statsStage.runJob();
      if (!statsJob.isSuccessful()) {
        throw new RuntimeException(
            String.format(
                "Computing stats for Stage %s had a problem",
                stage.getClass().getName()));
      }
    }
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
          "Ba10KExample");
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
        new Configuration(), new Ba10KExample(), args);
    System.exit(res);
  }
}
