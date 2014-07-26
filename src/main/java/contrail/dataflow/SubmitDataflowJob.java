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
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
import com.google.cloud.dataflow.sdk.values.TupleTag;

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


    ContrailParameters.addList(defs,  DataflowParameters.getDefinitions());
    return Collections.unmodifiableMap(defs);
  }


  /**
   * Output the contig as a string representing the fasta record.
   */
  protected static class RunBowtieDoFn extends DoFn<BowtieInput, String> {
    private final String imagePath;
    private final String localImage;

    public RunBowtieDoFn(String imagePath, String localImage) {
      this.imagePath = imagePath;
      this.localImage = localImage;
    }

    private String fetchImage() {
      // Copy the docker image
     ProcessBuilder builder = new ProcessBuilder("gsutil", "cp", imagePath, localImage);
     ShellUtil.runProcess(
    		 builder, "build-index", "", sLogger, null);
//     Process p;
//     try {
//       p = builder.start();
//       p.waitFor();
//     } catch (IOException | InterruptedException e) {
//       // TODO Auto-generated catch block
//       e.printStackTrace();
//       throw new UserCodeException(e);
//     }

     return localImage;
   }

   private void loadImage(String localImage) {
     // Load the docker image
     ProcessBuilder builder = new ProcessBuilder("docker", "load", "--input", localImage);
     ShellUtil.runProcess(
    		 builder, "load image: ", "", sLogger, null);
//     Process p;
//     try {
//       p = builder.start();
//       p.waitFor();
//     } catch (IOException | InterruptedException e) {
//       // TODO Auto-generated catch block
//       e.printStackTrace();
//     }
   }

    @Override
    public void startBatch(DoFn.Context c) {
      fetchImage();
      loadImage(localImage);
    }

    @Override
    public void processElement(ProcessContext c) {
      String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();
      BowtieInput input = c.element();
      String refFile = FilenameUtils.concat(tempDir, "ref.fasta");
      String queryFile = FilenameUtils.concat(tempDir, "query.fastq");
      try {
        FileOutputStream refStream = new FileOutputStream(refFile);
        for (Read read : input.getReference()) {
          FastUtil.writeFastARecord(refStream, read.getFasta());
        }
        refStream.close();

        FileOutputStream queryStream = new FileOutputStream(
            FilenameUtils.concat(tempDir, "query.fastq"));
        for (Read read : input.getQuery()) {
          FastUtil.writeFastQRecord(queryStream, read.getFastq());
        }
        queryStream.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new UserCodeException(e);
      }

      String containerDir = "/container_data";
      String containerIndexFile = FilenameUtils.concat(containerDir, "bowtie.index");
      String imageName = "contrail/bowtie";
      String containerRefFile = FilenameUtils.concat(containerDir, "ref.fasta");
      String containerQueryFile = FilenameUtils.concat(containerDir, "query.fastq");
      String command = String.format("/git_bowtie/bowtie-build %s %s",
          containerRefFile, containerIndexFile);
      
      ProcessBuilder builder = new ProcessBuilder(
    		  "docker", "run", "-t",
    		  "-v", tempDir + ":" + containerDir,
    		  imageName,
    		  "/git_bowtie/bowtie-build",
    		  containerRefFile,
    		  containerIndexFile);
      ShellUtil.runProcess(
    		  builder, "bowtie-index", "", sLogger, null);

      String outputFile = FilenameUtils.concat(tempDir, "bowtie.output");
      String containerOutputFile = FilenameUtils.concat(containerDir, "bowtie.output");

      ProcessBuilder bowtieBuilder = new ProcessBuilder(
          "docker", "run", "-t",
          "-v", tempDir + ":" + containerDir,
          imageName, "/git_bowtie/bowtie",
    	  "-q", "-v", "1", "-M", "2",
          containerIndexFile,
          containerQueryFile,
          containerOutputFile);
      Process bowtieProcess;
      ShellUtil.runProcess(
			bowtieBuilder, "run-bowtie", "", sLogger, null);


      try {
        FileReader inputStream = new FileReader(outputFile);
        BufferedReader reader = new BufferedReader(inputStream);
        String line = reader.readLine();;
        while(line != null) {          
          c.output(line);
          line = reader.readLine();
        }
        reader.close();
        inputStream.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        throw new UserCodeException(e);
      }
    }
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    Date now = new Date();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-hhmmss");
    // N.B. We don't use FilenameUtils.concat because it messes up the URI
    // prefix.
    String outputPath = (String) stage_options.get("outputpath");
    if (!outputPath.endsWith("/")) {
      outputPath += "/";
    }
    outputPath += formatter.format(now);
    outputPath += "/";

    PipelineOptions options = new PipelineOptions();
    options.diskSourceImage = "https://www.googleapis.com/compute/v1/projects/google-containers/global/images/container-vm-v20140710";
    
    if (stage_options.get("jobName") != null) {
      options.jobName = (String) stage_options.get("jobName");
    }
    DataflowParameters.setPipelineOptions(stage_options, options);

    Pipeline p = Pipeline.create();

    DataflowUtil.registerAvroCoders(p);

    PCollection<BowtieInput> bowtieInputs = ReadAvroSpecificDoFn.readAvro(
        BowtieInput.class, p, options, inputPath);

    String imagePath = "gs://contrail/docker_images/contrail.bowtie.tar";
    String localImage = "/tmp/contrail.bowtie.tar";
    PCollection<String> mappings = bowtieInputs.apply(ParDo.of(
        new RunBowtieDoFn(imagePath, localImage)).named("RunBowtie"));

    String mappingOutputs = outputPath + "bowtie.mappings";
    
    PipelineRunner runner = PipelineRunner.fromOptions(options);
    // If running on the service use a sharded output.
    if (DataflowPipelineRunner.class.isAssignableFrom(runner.getClass())) {
    	mappingOutputs += "@*";
    }
    
    mappings.apply(TextIO.Write.named("WriteMappings").to(mappingOutputs));

    p.run(runner);

    sLogger.info("Output written to: " + outputPath);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new SubmitDataflowJob(), args);
    System.exit(res);
  }
}
