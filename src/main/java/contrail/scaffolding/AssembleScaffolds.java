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
package contrail.scaffolding;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.ShellUtil;

/**
 * Assembly the scaffolds.
 */
public class AssembleScaffolds extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      AssembleScaffolds.class);
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    BuildBambusInput bambusInputStage = new BuildBambusInput();
    definitions.putAll(bambusInputStage.getParameterDefinitions());


    ParameterDefinition amosPath =
        new ParameterDefinition(
            "amos_path", "The directory where the amos tools are installed.",
            String.class, null);

    for (ParameterDefinition def: new ParameterDefinition[] {amosPath}) {
      definitions.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(definitions);
  }

  @Override
  public RunningJob runJob() throws Exception {
    // TODO(jeremy@lewi.us) we should check if all the parameters
    // required by all the child sub stages are supplied.
    String[] required_args = {"outputpath", "amos_path"};
    checkHasParametersOrDie(required_args);

    BuildBambusInput bambusInputStage = new BuildBambusInput();
    // Make a shallow copy of the stage options required by the stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            bambusInputStage.getParameterDefinitions().values());
    bambusInputStage.setParameters(stageOptions);
    bambusInputStage.runJob();

    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    String bankPath = FilenameUtils.concat(outputPath, "bank.bnk");

    sLogger.info("Load the data into amos.");
    String loadCommand = String.format(
        "%s/toAmos_new -s %s -m %s -c %s -b %s", amosPath,
        bambusInputStage.getFastaOutputFile(),
        bambusInputStage.getLibraryOutputFile(),
        bambusInputStage.getContigOutputFile(), bankPath);

    if (ShellUtil.execute(loadCommand, "toAmos_new:", sLogger) != 0) {
      sLogger.fatal(
          "Failed to load the bambus input into the amos bank",
          new RuntimeException("Failed to load bambus input into amos bank."));
    }

    sLogger.info("Executing bambus.");
    // TODO(jeremy@lewi.us): We should write all bambus output to a
    // subdirectory of output.
    String bambusCommand = String.format(
        "%s/goBambus2 %s bambus_output clk bundle reps, " +
        "-\"noPathRepeats\" orient, \"-maxOverlap 500 -redundancy 0\" " +
        "2fsta printscaff", amosPath, bankPath);


    if (ShellUtil.execute(bambusCommand, "goBambus2:", sLogger) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("Bambus failed."));
    }
    // TODO(jeremy@lewi.us): Process the data.
    return null;
  }

  public static void main(String[] args) throws Exception {
    AssembleScaffolds stage = new AssembleScaffolds();
    int res = stage.run(args);
    System.exit(res);
  }
}
