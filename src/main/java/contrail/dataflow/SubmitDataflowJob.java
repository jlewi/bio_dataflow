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
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.DockerClient.LogsParameter;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;

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
  
  private DockerClient docker;
  private String googleRegistryId;
  
  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    return Collections.unmodifiableMap(defs);
  }

  private void startDocker() {
    // Docker must be using a tcp port to work with the spotify client.
    String dockerAddress = "http://127.0.0.1:4243";
    docker = new DefaultDockerClient(dockerAddress);       
  }
  
  private void startGoogleRegistry() {
    // Get the Google docker image.
    String googleRegistryImage = "google/docker-registry";
    try {
      docker.pull(googleRegistryImage);
    } catch (DockerException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    
    // TODO(jlewi): How to determine an unused port.
    // TODO(jlewi): This assumes authorization is granted via a service account.
    
    // The google/docker-registry container is configured to use the port 5000.
    String registryPort = "5000";
    // The host can pick any available port.
    // TODO(jlewi): Figure out how to pick an available port.
    String hostPort = "5000";
    ContainerConfig config = ContainerConfig.builder()
        .image("google/docker-registry")
        //.cmd("google/docker-registry")
        .env("GCS_BUCKET=biocloudops-docker")
        .attachStderr(false)
        .attachStdout(false)
        .attachStdin(false)
        .portSpecs(hostPort + ":" + registryPort)
        .build();
        
    ContainerCreation creation;
    // The id of the container running the google docker registry.
    googleRegistryId = "";
    try {
      creation = docker.createContainer(config);
      googleRegistryId = creation.id();
      docker.startContainer(googleRegistryId);
    } catch (DockerException | InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }              
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
    
    startDocker();
 
    
    // Stop the registry.
    // TODO(jlewi): We need to figure out how to make sure this always runs
    // to avoid leaving the google registry running.
    try {
      docker.killContainer(googleRegistryId);
      docker.removeContainer(googleRegistryId);
    } catch (DockerException | InterruptedException e) {
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
