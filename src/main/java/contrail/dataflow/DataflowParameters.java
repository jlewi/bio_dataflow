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
import java.util.List;
import java.util.Map;

import com.google.cloud.dataflow.sdk.runners.PipelineOptions;

import contrail.stages.ParameterDefinition;

/**
 * Helper class for getting common parameters related to Dataflow.
 */
public class DataflowParameters {
  public static List<ParameterDefinition> getDefinitions() {
    ArrayList<ParameterDefinition> defs = new ArrayList<ParameterDefinition>();
    ParameterDefinition project =
        new ParameterDefinition(
            "project", "The google cloud project to use when running on  " +
            "Dataflow.", String.class, null);

    defs.add(project);

    ParameterDefinition stagingLocation =
        new ParameterDefinition(
            "stagingLocation", "Location on GCS where files should be staged.",
            String.class, null);
    defs.add(stagingLocation);

    ParameterDefinition dataflowEndpoint =
        new ParameterDefinition(
            "dataflowEndpoint", "Dataflow endpoint",
            String.class, null);
    defs.add(dataflowEndpoint);

    ParameterDefinition numWorkers =
        new ParameterDefinition(
            "num_workers", "Numer of workers.",
            Integer.class, 3);
    defs.add(numWorkers);

    ParameterDefinition apiRootUrl =
        new ParameterDefinition(
            "apiRootUrl", "Root url.",
            String.class, null);
    defs.add(apiRootUrl);

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
    options.runner = (String) stageOptions.get("runner");
    options.project = (String) stageOptions.get("project");
    options.stagingLocation = (String) stageOptions.get("stagingLocation");
    options.numWorkers = (Integer) stageOptions.get("num_workers");

    if (stageOptions.get("dataflowEndpoint") != null) {
      options.dataflowEndpoint =
          (String) (stageOptions.get("dataflowEndpoint"));
    }

    if (stageOptions.get("apiRootUrl") != null) {
      options.apiRootUrl =
          (String) (stageOptions.get("apiRootUrl"));
    }
  }
}
