/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package contrail.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.BlockingDataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;

import contrail.stages.ParameterDefinition;

/**
 * Helper class for getting common parameters related to Dataflow.
 */
public class DataflowParameters {
  public static List<ParameterDefinition> getDefinitions() {
    ArrayList<ParameterDefinition> defs = new ArrayList<ParameterDefinition>();
    // TODO(jeremy@lewi.us): We should make project and stagingLocation
    // required when executing on the service.
    ParameterDefinition project =
        new ParameterDefinition(
            "project", "The google cloud project to use when running on  " +
            "Dataflow.", String.class, "");

    defs.add(project);

    ParameterDefinition stagingLocation =
        new ParameterDefinition(
            "stagingLocation", "Location on GCS where files should be staged.",
            String.class, "");
    defs.add(stagingLocation);

    ParameterDefinition dataflowEndpoint =
        new ParameterDefinition(
            "dataflowEndpoint", "Dataflow endpoint",
            String.class, "");
    defs.add(dataflowEndpoint);

    ParameterDefinition numWorkers =
        new ParameterDefinition(
            "num_workers", "Numer of workers.",
            Integer.class, 3);
    defs.add(numWorkers);

    ParameterDefinition apiRootUrl =
        new ParameterDefinition(
            "apiRootUrl", "Root url.",
            String.class, "");
    defs.add(apiRootUrl);

    ParameterDefinition jobName =
        new ParameterDefinition(
            "jobName", "Name of the job.",
            String.class, "");
    defs.add(jobName);

    ParameterDefinition experiments =
        new ParameterDefinition(
            "experiments", "Comma separated list of experiments to enable.",
            String.class, "");
    defs.add(experiments);

    ParameterDefinition runner = new ParameterDefinition(
        "runner", "The pipeline runner to use.", String.class,
        "DirectPipelineRunner");
     defs.add(runner);

    return defs;
  }

  /**
   * Set pipeline options based on common dataflow parameters passed in
   * stageOptions.
   * @param stageOptions
   * @param options
   */
  public static void setPipelineOptions(
      Map<String, Object> stageOptions,
      PipelineOptions options) {
    // TODO(jeremy@lewi.us): Is there a way to reuse the parsing in
    // PipelineOptionsFactory and not have to duplicate all of the
    // dataflow pipeline options and parsing log ourselves?
    DataflowPipelineOptions dataflowOptions =
        options.as(DataflowPipelineOptions.class);

    if (!((String)stageOptions.get("stagingLocation")).isEmpty()) {
      dataflowOptions.setStagingLocation(
          (String) stageOptions.get("stagingLocation"));
    }

    if (stageOptions.get("num_workers") != null) {
      dataflowOptions.setNumWorkers(
          (Integer) stageOptions.get("num_workers"));
    }

    if (!((String)stageOptions.get("dataflowEndpoint")).isEmpty()) {
      dataflowOptions.setDataflowEndpoint(
          (String) stageOptions.get("dataflowEndpoint"));
    }

    if (!((String)stageOptions.get("apiRootUrl")).isEmpty()) {
      dataflowOptions.setApiRootUrl(
          (String) stageOptions.get("apiRootUrl"));
    }

    if (!((String)(stageOptions.get("jobName"))).isEmpty()) {
      dataflowOptions.setJobName((String) stageOptions.get("jobName"));
    }

    if (!((String)stageOptions.get("project")).isEmpty()) {
      dataflowOptions.setProject((String) stageOptions.get("project"));
    }

    String runner = (String) stageOptions.get("runner");
    if (runner != null) {
      if (runner.equals("DirectPipelineRunner")) {
        dataflowOptions.setRunner(DirectPipelineRunner.class);
      } else if (runner.equals("DataflowPipelineRunner")) {
        dataflowOptions.setRunner(DataflowPipelineRunner.class);
      } else if (runner.equals("BlockingDataflowPipelineRunner")) {
        dataflowOptions.setRunner(BlockingDataflowPipelineRunner.class);
      } else {
        throw new RuntimeException("Unrecorginzed Runner:" + runner);
      }
    }

    if (!((String)stageOptions.get("experiments")).isEmpty()) {
      dataflowOptions.setExperiments(new ArrayList<String>());
      String experiments = (String) (stageOptions.get("experiments"));
      dataflowOptions.getExperiments().addAll(Arrays.asList(
          experiments.split(",")));
    }
  }
}
