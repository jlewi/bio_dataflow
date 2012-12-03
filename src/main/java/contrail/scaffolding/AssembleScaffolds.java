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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
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
    String bankName = "bank.bnk";
    String bankPath = FilenameUtils.concat(outputPath, bankName);

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

    // It looks like goBambus2 can't take the path to the bank. The script
    // needs to be executed from the directory containing the bank.
    ArrayList<String> bambusCommand = new ArrayList<String>();
    bambusCommand.add(FilenameUtils.concat(amosPath, "goBambus2"));
    bambusCommand.add(bankName);
    bambusCommand.add("bambus_output");
    bambusCommand.add("clk");
    bambusCommand.add("bundle");
    bambusCommand.add("reps,\"-noPathRepeats\"");
    //bambusCommand.add("orient,\"-maxOverlap 500 -redundancy 0\"");
    //bambusCommand.add("orient,\"-redundancy 0\"");
    //bambusCommand.add("2fasta");
    //bambusCommand.add("printscaff");

    String debug = StringUtils.join(bambusCommand, " ");
    if (ShellUtil.execute(
            bambusCommand, outputPath, "goBambus2:", sLogger) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("Bambus failed."));
      System.exit(-1);
    }
    // Make a copy of the bambus log because when we rerun bambus below
    // the log will get overwritten.
    FileUtils.copyFile(
        new File(outputPath, "bambus2.log"),
        new File(outputPath, "bambus2.log.initial_stages"));
    // We manually run the oriengt, 2fasta, and printscaff stages rather
    // than using the goBambus binary because we ran into issues with
    // the goBambus script and the parsing of the arguments for the orient
    // stage.
    ArrayList<String> orientCommand = new ArrayList<String>();
    orientCommand.add(FilenameUtils.concat(amosPath, "OrientContigs"));
    orientCommand.add("-b");
    orientCommand.add(bankPath);
    orientCommand.add("-maxOverlap");
    orientCommand.add("500");
    orientCommand.add("-redundancy");
    orientCommand.add("0");

    if (ShellUtil.execute(
        orientCommand, outputPath, "goBambus2:", sLogger) != 0) {
        sLogger.fatal(
            "Bambus OrientContigs failed.",
            new RuntimeException("Bambus failed."));
        System.exit(-1);
    }

    // Run the stages to print the output.
    ArrayList<String> bambusOutput = new ArrayList<String>();
    bambusOutput.add(FilenameUtils.concat(amosPath, "goBambus2"));
    bambusOutput.add(bankName);
    bambusOutput.add("bambus_output");
    bambusOutput.add("2fasta");
    bambusOutput.add("printscaff");

    if (ShellUtil.execute(
        bambusOutput, outputPath, "goBambus2:", sLogger) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("Bambus failed."));
      System.exit(-1);
    }

    // Run the stages
    // TODO(jeremy@lewi.us): Process the data.
    return null;
  }

  public static void main(String[] args) throws Exception {
    AssembleScaffolds stage = new AssembleScaffolds();
    int res = stage.run(args);
    System.exit(res);
  }
}
