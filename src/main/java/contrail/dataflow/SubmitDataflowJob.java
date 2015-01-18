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

import contrail.stages.NonMRStage;


/**
 * Submit a Dataflow job.
 *
 * This is a simple Dataflow job which submits another Dataflow job
 * by running that job's main program.
 */
public class SubmitDataflowJob extends NonMRStage {
  // TODO(jeremy@lewi.us): 2015-01-18 the code below needs to be updated to
  // work with the newest SDK or deleted.
//  private static final Logger sLogger = Logger.getLogger(
//      SubmitDataflowJob.class);
//
//
//  /**
//   *  creates the custom definitions that we need for this phase
//   */
//  @Override
//  protected Map<String, ParameterDefinition> createParameterDefinitions() {
//    HashMap<String, ParameterDefinition> defs =
//        new HashMap<String, ParameterDefinition>();
//    defs.putAll(super.createParameterDefinitions());
//
//    ContrailParameters.addList(defs, DataflowParameters.getDefinitions());
//    return Collections.unmodifiableMap(defs);
//  }
//
//  @Override
  @Override
  protected void stageMain() {
//    PipelineOptions options = PipelineOptionsFactory.create();
//    DataflowParameters.setPipelineOptions(stage_options, options);
//
//    Pipeline p = Pipeline.create(options);
//
//    List<String> serviceCommand = Arrays.asList(
//        "com.google.cloud.dataflow.examples.WordCount",
//        "--runner", "BlockingDataflowPipelineRunner",
//        "--gcloudPath", "/google-cloud-sdk/bin/gcloud",
//        "--project", "biocloudops",
//        "--stagingLocation", "gs://dataflow-dogfood2-jlewi/staging",
//        "--input", "gs://dataflow-dogfood2-jlewi/nytimes-index.html",
//        "--output", "gs://dataflow-dogfood2-jlewi/tmp/nytimes-counts.txt");
//
//    List<String> localCommand = Arrays.asList(
//        "com.google.cloud.dataflow.examples.WordCount",
//        "--runner", "DirectPipelineRunner", "--input", "/tmp/words",
//        "--output", "/tmp/word-count.txt");
//
//    String bucket = "biocloudops-temp";
//    String objectPath = "examples-1-20140730.jar";
//    List<String> jars = Arrays.asList("someJar");
//    String imageName = "localimagename";
//
//    DataflowJobSpec jobToRun = new DataflowJobSpec(
//        localCommand, jars, imageName);
//
//    PObject<DataflowJobSpec> jobSpec = p.begin().apply(CreatePObject.of(
//        jobToRun)).setCoder(AvroCoder.of(DataflowJobSpec.class));
//
//    jobSpec.apply(new RunDataflowJob());
//    p.run();
  }
//
//  public static void main(String[] args) throws Exception {
//    int res = ToolRunner.run(
//        new Configuration(), new SubmitDataflowJob(), args);
//    System.exit(res);
//  }
}
