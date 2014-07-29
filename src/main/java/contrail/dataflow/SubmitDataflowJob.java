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

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;

import contrail.sequences.FastUtil;
import contrail.sequences.Read;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.FileHelper;
import contrail.util.ShellUtil;


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

//    ContrailParameters.add(defs, new ParameterDefinition(
//        "jar", "The GCS path of the jar to run.",
//            String.class, null));
//    ContrailParameters.add(defs, new ParameterDefinition(
//        "main_class", "The full class path of the main program.",
//            String.class, null));
//    ContrailParameters.add(defs, new ParameterDefinition(
//        "arguments", "The arguments to pass to the invocation.",
//            String.class, null));
//
//    ContrailParameters.addList(defs,  DataflowParameters.getDefinitions());
    return Collections.unmodifiableMap(defs);
  }

  @Override
  protected void stageMain() {
    List<String> serviceCommand = Arrays.asList(
        "java", "-cp",  "/cloud-dataflow/target/examples-1.jar",
        "com.google.cloud.dataflow.examples.WordCount",
        "--runner", "BlockingDataflowPipelineRunner",
        "--gcloudPath", "/google-cloud-sdk/bin/gcloud",
        "--project", "biocloudops",
        "--stagingLocation", "gs://dataflow-dogfood2-jlewi/staging",
        "--input", "gs://dataflow-dogfood2-jlewi/nytimes-index.html",
        "--output", "gs://dataflow-dogfood2-jlewi/tmp/nytimes-counts.txt");
    
    List<String> localCommand = Arrays.asList(
        "java", "-cp",  "/cloud-dataflow/target/examples-1.jar",
        "com.google.cloud.dataflow.examples.WordCount",
        "--runner", "DirectPipelineRunner", "--input", "/tmp/words",
        "--output", "/tmp/word-count.txt");
    
    // Docker must be using a tcp port.
    String dockerAddress = "http://127.0.0.1:4243";
    DockerClient docker = new DefaultDockerClient(dockerAddress);
    DockerProcessBuilder builder =  new DockerProcessBuilder(localCommand, docker);
    builder.setImage("contrail/dataflow");
    
    
    try {
      DockerProcess process = builder.start();
      
      // process.waitForAndLogProcess(sLogger, null);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      throw new RuntimeException(e);
    } catch (DockerException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new SubmitDataflowJob(), args);
    System.exit(res);
  }
}
