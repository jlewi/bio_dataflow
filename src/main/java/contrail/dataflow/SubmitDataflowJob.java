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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.CreatePObject;
import com.google.cloud.dataflow.sdk.values.PObject;

import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;


/**
 * Submit a Dataflow job.
 *
 * This is a simple Dataflow job which submits another Dataflow job
 * by running that job's main program.
 */
public class SubmitDataflowJob extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      SubmitDataflowJob.class);


  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ContrailParameters.addList(defs, DataflowParameters.getDefinitions());
    return Collections.unmodifiableMap(defs);
  }

  @Override
  protected void stageMain() {
    PipelineOptions options = new PipelineOptions();
    DataflowParameters.setPipelineOptions(stage_options, options);

    Pipeline p = Pipeline.create(options);

    List<String> serviceCommand = Arrays.asList(
        "com.google.cloud.dataflow.examples.WordCount",
        "--runner", "BlockingDataflowPipelineRunner",
        "--gcloudPath", "/google-cloud-sdk/bin/gcloud",
        "--project", "biocloudops",
        "--stagingLocation", "gs://dataflow-dogfood2-jlewi/staging",
        "--input", "gs://dataflow-dogfood2-jlewi/nytimes-index.html",
        "--output", "gs://dataflow-dogfood2-jlewi/tmp/nytimes-counts.txt");

    List<String> localCommand = Arrays.asList(
        "com.google.cloud.dataflow.examples.WordCount",
        "--runner", "DirectPipelineRunner", "--input", "/tmp/words",
        "--output", "/tmp/word-count.txt");

    String bucket = "biocloudops-temp";
    String objectPath = "examples-1-20140730.jar";
    List<String> jars = Arrays.asList("someJar");
    String imageName = "localimagename";

    DataflowJobSpec jobToRun = new DataflowJobSpec(
        localCommand, jars, imageName);

    PObject<DataflowJobSpec> jobSpec = p.begin().apply(CreatePObject.of(
        jobToRun)).setCoder(AvroCoder.of(DataflowJobSpec.class));

    jobSpec.apply(new RunDataflowJob());
    p.run();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new SubmitDataflowJob(), args);
    System.exit(res);
  }
}
