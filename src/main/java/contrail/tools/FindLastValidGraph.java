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

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import contrail.stages.BuildGraphAvro;
import contrail.stages.CompressAndCorrect;
import contrail.stages.ContrailParameters;
import contrail.stages.PairMergeAvro;
import contrail.stages.ParameterDefinition;
import contrail.stages.PopBubblesAvro;
import contrail.stages.QuickMergeAvro;
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

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    // Over the comment for the inputpath.
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

      JsonFactory factory = new JsonFactory();
      //JsonGenerator generator = factory.createJsonGenerator(outStream);
      //generator.setPrettyPrinter(new DefaultPrettyPrinter());
      JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          stageInfo.getSchema(), stream);
      //encoder.configure(generator);
      SpecificDatumReader<StageInfo> reader =
          new SpecificDatumReader<StageInfo>(StageInfo.class);
      reader.read(stageInfo, decoder);
      stream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }
  }

  /**
   * Return a set of the names of the stages that produce graphs to be checked.
   * @return
   */
  private HashSet<String> listOfStages() {
    HashSet<String> classNames = new HashSet<String>();

    Stage[] validStages =
      {new BuildGraphAvro(), new CompressAndCorrect(), new PopBubblesAvro(),
        new PairMergeAvro(), new QuickMergeAvro()};

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

  private void findLastValidGraph() {
    // When formatting the step as a string we want to zero pad it
    DecimalFormat sf = new DecimalFormat("00");

    StageInfo pipelineInfo = loadStageInfo();

    // Get the stages in reverse order.
    ArrayList<StageInfo> subStages = new ArrayList<StageInfo>();
    subStages.addAll(pipelineInfo.getSubStages());
    Collections.reverse(subStages);

    // List of the stages which produce graphs to evaluate.
    HashSet<String> stageNames = listOfStages();

    int stepNum = 1;

    boolean foundValid = false;

    // TODO(jlewi): Stages like CompressAndCorrect which are themselves
    // pipelines should set their StageInfo to include substages.
    // We should then modify this process so that we include those stages
    // here.
    for (StageInfo subStage : subStages) {
      if (!stageNames.contains(subStage.getStageClass())) {
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
      parameters.put("outputpath", String.format(
          "step-%s-%s",sf.format(stepNum), subStage.getStageClass()));

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
        foundValid = true;
        break;
      }
      sLogger.info(String.format(
          "Stage %s. Number of errors: %d", subStage.getStageClass(),
          errorCount));
      ++stepNum;
    }

    if (!foundValid) {
      sLogger.info("No stages produced valid graphs.");
      return;
    }

    // Print out information about the stage that corrupts the graph.
    int errorStep = stepNum -1;
    StageInfo errorStage = subStages.get(errorStep);

    sLogger.info(
        "StageInfo for stage corrupting the graph:\n" +  errorStage.toString());
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
        new Configuration(), new ValidateGraph(), args);
    System.exit(res);
  }
}
