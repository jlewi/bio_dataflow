/**
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
package contrail.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import contrail.stages.BuildGraphAvro;
import contrail.stages.CompressAndCorrect;
import contrail.stages.ContrailParameters;
import contrail.stages.PairMergeAvro;
import contrail.stages.ParameterDefinition;
import contrail.stages.PopBubblesAvro;
import contrail.stages.QuickMergeAvro;
import contrail.stages.RemoveTipsAvro;
import contrail.stages.Stage;
import contrail.stages.StageInfo;
import contrail.stages.StageParameter;
import contrail.stages.ValidateGraph;

/**
 * A tool for finding the stage when a graph became corrupted.
 *
 * This tool helps find bugs that cause the graph to become invalid.
 * This tool works by taking as input the json file containing the StageInfo
 * outputted by a pipeline. We then iteratively run ValidateGraph on
 * the outputs of each stage in reverse order until we find the last
 * stage which produced a valid graph.
 */
public class FindLastValidGraph extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      FindLastValidGraph.class);

  // Store the info for the stage causing problems.
  private StageInfo errorStage;

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    // Delete the parameter K.
    defs.remove("K");

    // Overwrite the comment for the inputpath.
    ParameterDefinition input = new ParameterDefinition(
        "inputpath", "The path to the json file encoding StageInfo.",
        String.class,
        null);

    defs.put(input.getName(), input);
    return Collections.unmodifiableMap(defs);
  }

  private StageInfo loadStageInfo() {
    String inputPath = (String) this.stage_options.get("inputpath");
    StageInfo stageInfo = new StageInfo();

    try {
      FileSystem fs = FileSystem.get(this.getConf());

      FSDataInputStream  stream = fs.open(new Path(inputPath));
      JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          stageInfo.getSchema(), stream);
      SpecificDatumReader<StageInfo> reader =
          new SpecificDatumReader<StageInfo>(StageInfo.class);
      reader.read(stageInfo, decoder);
      stream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }
    return stageInfo;
  }

  /**
   * Return a set of the names of the stages that produce graphs to be checked.
   * @return
   */
  private HashSet<String> listOfStages() {
    HashSet<String> classNames = new HashSet<String>();

    Stage[] validStages =
      {new BuildGraphAvro(), new CompressAndCorrect(), new PopBubblesAvro(),
        new PairMergeAvro(), new RemoveTipsAvro(), new QuickMergeAvro()};

    for (Stage validStage : validStages) {
      classNames.add(validStage.getClass().getName());
    }
    return classNames;
  }

  /**
   * Return the value of the specified parameter or null if it isn't found.
   * @param stage
   * @param name
   * @return
   */
  private String getStageParameter(StageInfo stage, String name) {
    String value = null;
    for (StageParameter parameter  : stage.getParameters()) {
      if (parameter.getName().toString().equals(name)) {
        value = parameter.getValue().toString();
        break;
      }
    }
    return value;
  }

  /**
   * Convert the stageinfo to a json string.
   */
  private String stageInfoToString(StageInfo info) {
    ByteArrayOutputStream byteStream = null;
    try {
      byteStream = new ByteArrayOutputStream();

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(byteStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          info.getSchema(), generator);
      SpecificDatumWriter<StageInfo> writer =
          new SpecificDatumWriter<StageInfo>(StageInfo.class);
      writer.write(info, encoder);
      // We need to flush it.
      encoder.flush();
      byteStream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }

    return  byteStream.toString();
  }

  private void findLastValidGraph() {
    // When formatting the step as a string we want to zero pad it
    DecimalFormat sf = new DecimalFormat("00");

    StageInfo pipelineInfo = loadStageInfo();

    // Get the value of K for the pipeline.
    String kAsString = getStageParameter(pipelineInfo, "K");
    if (kAsString == null) {
      sLogger.fatal(
          "Could not determine the value of K from the json file.",
          new RuntimeException("Couldn't get K."));
      System.exit(-1);
    }
    Integer K = Integer.parseInt(kAsString);

    // Get the stages in reverse order.
    ArrayList<StageInfo> subStages = new ArrayList<StageInfo>();
    subStages.addAll(pipelineInfo.getSubStages());
    Collections.reverse(subStages);

    // List of the stages which produce graphs to evaluate.
    HashSet<String> stageNames = listOfStages();
    boolean foundValid = false;

    int errorStageIndex = -1;
    // TODO(jlewi): Stages like CompressAndCorrect which are themselves
    // pipelines should set their StageInfo to include substages.
    // We should then modify this process so that we include those stages
    // here.
    for (int stageIndex = 0; stageIndex < subStages.size(); ++stageIndex) {
      StageInfo subStage = subStages.get(stageIndex);
      if (!stageNames.contains(subStage.getStageClass().toString())) {
        sLogger.info("Skipping the stage:" + subStage.getStageClass());
        continue;
      }

      String outputPath = getStageParameter(subStage, "outputpath");

      if (outputPath == null) {
        sLogger.fatal(
            "Could not determine the outputpath for stage:" +
            subStage.getStageClass(),
            new RuntimeException("Couldn't get outputpath."));
        System.exit(-1);
      }
      sLogger.info(String.format(
          "Checking stage:%s which produced:%s", subStage.getStageClass(),
          outputPath));

      ValidateGraph validateStage = new ValidateGraph();
      HashMap<String, Object> parameters = new HashMap<String, Object>();
      parameters.put("inputpath", outputPath);
      String subDir = String.format(
          "step-%s-%s",sf.format(errorStageIndex), subStage.getStageClass());
      parameters.put(
          "outputpath", FilenameUtils.concat(outputPath, subDir));
      parameters.put("K", K);
      validateStage.setParameters(parameters);
      RunningJob job  = null;
      try {
        job = validateStage.runJob();
      } catch(Exception e) {
        sLogger.fatal("There was a problem validating the graph.", e);
        System.exit(-1);
      }

      long errorCount = ValidateGraph.getErrorCount(job);
      if (errorCount == 0) {
        errorStageIndex = stageIndex -1;
        foundValid = true;
        break;
      }
      sLogger.info(String.format(
          "Stage %s. Number of errors: %d", subStage.getStageClass(),
          errorCount));
      ++errorStageIndex;
    }

    if (!foundValid) {
      sLogger.info("No stages produced valid graphs.");
      return;
    }

    // Print out information about the stage that corrupts the graph.
    errorStage = subStages.get(errorStageIndex);

    sLogger.info(
        "StageInfo for stage corrupting the graph:\n" +
        stageInfoToString(errorStage));
  }

  /**
   * Return the information for the stage corrupting the graph.
   *
   * This function must be called after executing the stage.
   */
  public StageInfo getErrorStage() {
    return errorStage;
  }

  @Override
  public RunningJob runJob() throws Exception {
    // Check for missing arguments.
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }
    initializeJobConfiguration(conf);

    findLastValidGraph();

    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new FindLastValidGraph(), args);
    System.exit(res);
  }
}
