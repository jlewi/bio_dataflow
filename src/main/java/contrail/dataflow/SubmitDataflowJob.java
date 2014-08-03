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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
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
import com.google.cloud.dataflow.sdk.util.Credentials;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerRequestException;
import com.spotify.docker.client.LogMessage;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.DockerClient.LogsParameter;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;

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
  private String localTempFileName;
  
  /* The local name of the registry. */
  private String registryLocalName;
  
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
    } catch (DockerRequestException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (DockerException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    // TODO(jlewi): How to determine an unused port.
    // TODO(jlewi): This assumes authorization is granted via a service account.
    
    // The google/docker-registry container is configured to use the port 5000.
    String registryPort = "5000";
    // The host can pick any available port.
    // TODO(jlewi): Figure out how to pick an available port.
    String hostPort = "5010";
    
    String exposedPort = registryPort + "/tcp";
    ContainerConfig config = ContainerConfig.builder()
        .image("google/docker-registry")
        .env("GCS_BUCKET=biocloudops-docker")
        .attachStderr(false)
        .attachStdout(false)
        .attachStdin(false)
        .portSpecs(exposedPort)
        //.exposedPorts(exposedPort)
        .build();
        
    registryLocalName = "localhost:" + hostPort;
    ContainerCreation creation;
    // The id of the container running the google docker registry.
    googleRegistryId = "";
    try {
      creation = docker.createContainer(config);
      googleRegistryId = creation.id();
      
      Map<String, List<PortBinding>> portBindings = new TreeMap<String, List<PortBinding>>();
      portBindings.put(exposedPort, new ArrayList<PortBinding>());
      
      // N.B. Trying to bind address 0.0.0.0 doesn't work. Maybe that address only works
      // if you are connected via the unix socket and not a tcp port.
      portBindings.get(exposedPort).add(PortBinding.of("127.0.0.1", Integer.parseInt(hostPort)));
      
      HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();
      docker.startContainer(googleRegistryId, hostConfig);
      
      while (!docker.inspectContainer(googleRegistryId).state().running()) {
        sLogger.info("Waiting for google docker registry to start.");
        Thread.sleep(1000);
      }
    }  catch (DockerRequestException e1) {
      sLogger.error(e1.message(), e1);
    } catch (DockerException e1) {      
      sLogger.error(e1);        
    } catch (InterruptedException e1) {
      sLogger.error(e1);
    }              
  }
  
  /**
   * Download the jar containing the user's code.
   * 
   * @param options
   */
  private void downloadJar(PipelineOptions options) {
    JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    String bucket = "biocloudops-temp";
    String objectPath = "examples-1-20140730.jar";
    
    String[] pieces = objectPath.split("/");
    String baseName = pieces[pieces.length - 1];
    try {
      File localTempFile;
      localTempFile = File.createTempFile("tmp", baseName);
      localTempFileName = localTempFile.getAbsolutePath();
      localTempFile.delete();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
        
    // Download the jar containing the code.
    // Initialize the transport.    
    NetHttpTransport httpTransport;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      // Authorization.
      Credential credential = Credentials.getWorkerCredential(options);

      // Set up global Storage instance.
      Storage storage = new Storage.Builder(httpTransport, JSON_FACTORY, credential)
      .setApplicationName(this.getClass().getSimpleName()).build();

      Storage.Objects.Get getObject = storage.objects().get(bucket, objectPath);

      // Downloading data.
      FileOutputStream out = new FileOutputStream(localTempFileName);
      // If you're not in AppEngine, download the whole thing in one request, if possible.
      getObject.getMediaHttpDownloader();
      getObject.executeMediaAndDownloadTo(out);

      out.close();
    } catch (GeneralSecurityException | IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }
  
  private void logLines(String[] lines) {
    if (lines.length == 0) {
      return;
    }
    
    sLogger.info(StringUtils.join(lines, "\n"));
  }
  
  protected void runJob() {
    String imageName = registryLocalName + "/contrail/dataflow";    
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
    
    ContainerConfig config = ContainerConfig.builder()
        .image(imageName)
        .cmd(serviceCommand)
        .attachStderr(true)
        .attachStderr(true)
        .build();
        
    ContainerCreation creation;
    try {
      // Fetch the image.
      // TODO(jeremy@lewi.us): I think there might be a race condition to
      // make sure the google-docker registry containter is fully started.
      int numTries = 0;
      final int maxRetries = 5;
      while (true) {
        ++numTries;
        try {
          docker.pull(imageName);
          break;
        } catch (DockerRequestException e) {
          if (numTries < maxRetries) {
            sLogger.info("Waiting for Google Docker Regiistry to start...");
            Thread.sleep(500);            
          } else {
            throw e;
          }
        }
      }
      creation = docker.createContainer(config);
      String id = creation.id();
      ContainerInfo info = docker.inspectContainer(id);
      docker.startContainer(id);
      //docker.waitContainer(id);
          
      // N.B. We don't get a single stream with both logs because
      // we want to keep track of how many lines we've read and only 
      // print the difference. Docker's FOLLOW capability won't seem to work with 
      // the remote API. 
      ContainerLogStream stdOutStream = new ContainerLogStream(
          docker, ContainerLogStream.DockerStream.STDOUT, id);
      ContainerLogStream stdErrStream = new ContainerLogStream(
          docker, ContainerLogStream.DockerStream.STDERR, id);
      while (docker.inspectContainer(id).state().running()) {      
        LogStream stdErr = docker.logs(id, LogsParameter.STDERR);

        logLines(stdOutStream.readNextLines());
        logLines(stdErrStream.readNextLines());
               
        // Sleep for a .25 seconds.
        Thread.sleep(250);
      }
      logLines(stdOutStream.readNextLines());
      logLines(stdErrStream.readNextLines());
      
      // Remove the container.
      docker.removeContainer(id);
    } catch (DockerRequestException e) {
      sLogger.error(e.message(), e);
    } catch (DockerException | InterruptedException e) {
    	
      // TODO Auto-generated catch block
      e.printStackTrace();
    }    
  }
  
  @Override
  protected void stageMain() {
    PipelineOptions options = new PipelineOptions();
       
    downloadJar(options);
 
    startDocker();
    startGoogleRegistry();
    
    runJob();
    
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
