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
import java.util.Arrays;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import contrail.stages.BuildGraphAvro;
import contrail.stages.CompressAndCorrect;
import contrail.stages.CompressChains;
import contrail.stages.ContrailParameters;
import contrail.stages.PairMergeAvro;
import contrail.stages.ParameterDefinition;
import contrail.stages.PipelineStage;
import contrail.stages.PopBubblesAvro;
import contrail.stages.QuickMergeAvro;
import contrail.stages.RemoveLowCoverageAvro;
import contrail.stages.RemoveTipsAvro;
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
public class FindLastValidGraph extends PipelineStage {
  private static final Logger sLogger = Logger.getLogger(
      FindLastValidGraph.class);

  // Store the info for the stage causing problems.
  private StageInfo errorStage;

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

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
      Path inPath = new Path(inputPath);
      FileSystem fs = inPath.getFileSystem(this.getConf());

      FSDataInputStream stream = fs.open(inPath);
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
   * Return the value of the specified parameter or null if it isn't found.
   * @param stage
   * @param name
   * @return
   */
  private String getStageParameter(StageInfo stage, String name) {
    String value = null;

    for (StageParameter parameter  : stage.getModifiedParameters()) {
      if (parameter.getName().toString().equals(name)) {
        value = parameter.getValue().toString();
        return value;
      }
    }

    for (StageParameter parameter  : stage.getParameters()) {
      if (parameter.getName().toString().equals(name)) {
        value = parameter.getValue().toString();
        return value;
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
    String validationDir = (String) stage_options.get("outputpath");

    // Get the value of K for the pipeline.
    String kAsString = getStageParameter(pipelineInfo, "K");
    if (kAsString == null) {
      sLogger.fatal(
          "Could not determine the value of K from the json file.",
          new RuntimeException("Couldn't get K."));
      System.exit(-1);
    }
    Integer K = Integer.parseInt(kAsString);

    // We need to do a depth first search.
    ArrayList<StageInfo> stageStack = new ArrayList<StageInfo>();

    stageStack.add(pipelineInfo);

    boolean foundValid = false;

    // Names of stages to treat as pipelines where we just add the substages.
    HashSet<String> pipelineStageNames = new HashSet<String>();
    pipelineStageNames.addAll(Arrays.asList(
        CompressAndCorrect.class.getName(), CompressChains.class.getName()));

    // Names of regular stages.
    HashSet<String> regularStages = new HashSet<String>();
    regularStages.addAll(Arrays.asList(
        BuildGraphAvro.class.getName(), QuickMergeAvro.class.getName(),
        PairMergeAvro.class.getName(), RemoveLowCoverageAvro.class.getName(),
        PopBubblesAvro.class.getName(), RemoveTipsAvro.class.getName()));

    errorStage = null;

    int step = 0;
    while (stageStack.size() > 0 && !foundValid) {
      ++step;
      StageInfo current = stageStack.remove(stageStack.size() - 1);

      String stageClass = current.getStageClass().toString();

      if (pipelineStageNames.contains(stageClass)) {
        stageStack.addAll(current.getSubStages());
        continue;
      }

      if (regularStages.contains(stageClass)) {
        String outputPath = getStageParameter(current, "outputpath");

        if (outputPath == null) {
          sLogger.fatal(
              "Could not determine the outputpath for stage:" +
              stageClass,
              new RuntimeException("Couldn't get outputpath."));
          System.exit(-1);
        }

        // Make sure the path exists.
        // Its possible the path exists but doesn't contain any avro files.
        // We handle that possibility by checking the input records counter
        // after the job runs.
        Path graphPath = new Path(outputPath);
        try {
          if (!graphPath.getFileSystem(getConf()).exists(graphPath)) {
            sLogger.info(String.format(
                "Stage %s: The output no longer exists for this stage.",
                stageClass));
            // Continue with the other stages.
            continue;
          }
        } catch (IOException e) {
          sLogger.fatal("Couldn't verify that path exists:" + outputPath, e);
        }
        sLogger.info(String.format(
            "Checking stage:%s which produced:%s", stageClass,
            outputPath));
        ValidateGraph validateStage = new ValidateGraph();
        validateStage.initializeAsChild(this);
        validateStage.setParameter("inputpath", outputPath);
        String subDir = String.format(
            "step-%s-%s",sf.format(step), stageClass);
        validateStage.setParameter(
            "outputpath", FilenameUtils.concat(validationDir, subDir));
        validateStage.setParameter("K", K);

        executeChild(validateStage);

        if (validateStage.getNumMapInputRecords() <= 0) {
          // The graph was empty. For now treat this as an error.
          sLogger.fatal(String.format(
              "Stage %s. The input didn't contain any nodes.", stageClass));
          System.exit(-1);
        }

        long errorCount = validateStage.getErrorCount();
        if (errorCount == 0) {
          foundValid = true;
          continue;
        } else {
          errorStage = current;
        }

        sLogger.info(String.format(
            "Stage %s. Number of errors: %d", stageClass, errorCount));
        continue;
      }

      if (current.getSubStages().size() > 0) {
        // Default to checking its substages.
        sLogger.info("Adding substages of stage:" + stageClass);
        stageStack.addAll(current.getSubStages());
        continue;
      }
      // Do nothing for all other stages.
      sLogger.info("Skipping stage:" + stageClass);
    }

    if (!foundValid) {
      sLogger.info("No stages produced valid graphs.");
      return;
    }

    if (errorStage != null) {
      sLogger.info(
          "Stage Corrupting the graph:\n" + stageInfoToString(errorStage));
    } else {
      if (foundValid) {
        sLogger.info(
            "The final stage produced a valid graph.");
      } else {
        sLogger.error(
            "Could not find the stage corrupting the graph.");
      }
    }
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
  protected void stageMain() {
    findLastValidGraph();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new FindLastValidGraph(), args);
    System.exit(res);
  }
}
