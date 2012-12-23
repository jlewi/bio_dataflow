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


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import contrail.stages.BuildGraphAvro;
import contrail.stages.CompressAndCorrect;
import contrail.stages.ContrailParameters;
import contrail.stages.FastqPreprocessorAvroCompressed;
import contrail.stages.GraphStats;
import contrail.stages.GraphToFasta;
import contrail.stages.NotImplementedException;
import contrail.stages.ParameterDefinition;
import contrail.stages.QuickMergeAvro;
import contrail.stages.Stage;
import contrail.stages.StageInfo;

/**
 * A pipeline for assembling the contigs.
 */
public class AssembleContigs extends Stage {
  private static final Logger sLogger = Logger.getLogger(AssembleContigs.class);

  public AssembleContigs() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    // We add all the options for the stages we depend on.
    Stage[] substages =
      {new FastqPreprocessorAvroCompressed(), new BuildGraphAvro(),
       new QuickMergeAvro(), new CompressAndCorrect(), new GraphStats()};

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    return Collections.unmodifiableMap(definitions);
  }

  protected void setDefaultParameters() {
    // This function is intended to be overloaded in subclasses which
    // customize the parameters for different datasets.
  }

  /**
   * Write the current value of stage info to a file.
   */
  private void writeStageInfo(StageInfo info) {
    // TODO(jlewi): We should cleanup old stage files after writing
    // the new one. Or we could try appending json records to the same file.
    // When I tried appending, the method fs.append threw an exception.
    String outputPath = (String) stage_options.get("outputpath");
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    Date date = new Date();
    String timestamp = formatter.format(date);

    String stageDir = FilenameUtils.concat(
        outputPath, "stage_info");

    String outputFile = FilenameUtils.concat(
        stageDir, "stage_info." + timestamp + ".json");
    try {
      FileSystem fs = FileSystem.get(this.getConf());
      if (!fs.exists(new Path(stageDir))) {
        fs.mkdirs(new Path(stageDir));
      }
      FSDataOutputStream outStream = fs.create(new Path(outputFile));

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          info.getSchema(), generator);
      SpecificDatumWriter<StageInfo> writer =
          new SpecificDatumWriter<StageInfo>(StageInfo.class);
      writer.write(info, encoder);
      // We need to flush it.
      encoder.flush();
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }
  }

  private void processGraph() throws Exception {
    StageInfo stageInfo = getStageInfo(null);

    // TODO(jlewi): Does this function really need to throw an exception?
    String outputPath = (String) stage_options.get("outputpath");

    Stage[] subStages =
      {new FastqPreprocessorAvroCompressed(), new BuildGraphAvro(),
       new QuickMergeAvro(), new CompressAndCorrect(), new GraphToFasta()};

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
      stageInfo.getSubStages().add(stage.getStageInfo(job));
      writeStageInfo(stageInfo);
      if (job !=null && !job.isSuccessful()) {
        throw new RuntimeException(
            String.format(
                "Stage %s had a problem", stage.getClass().getName()));
      }

      latestPath = stageOutput;
      // We compute graph stats for each stage except the conversion
      // of Fastq files to Avro and BuildGraph.
      if (FastqPreprocessorAvroCompressed.class.isInstance(stage) ||
          BuildGraphAvro.class.isInstance(stage) ||
          GraphToFasta.class.isInstance(stage)) {
        continue;
      }

      // TODO(jlewi): It would probably be better to continue running the
      // pipeline and not blocking on GraphStats.
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
      stageInfo.getSubStages().add(statsStage.getStageInfo(statsJob));
      writeStageInfo(stageInfo);
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
          "AssembleContigs");
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
        new Configuration(), new AssembleContigs(), args);
    System.exit(res);
  }
}
