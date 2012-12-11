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
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.ShellUtil;

/**
 * Assembly the scaffolds.
 *
 * Assembly uses bowtie to align the original reads to the contigs.
 * We convert the bowtie output, contigs, and reads into a format that
 * Bambus can use for scaffolding.
 *
 * Bambus takes as input 3 files. The first file is the contig file in
 * TIGR/GDE format:
 * see http://sourceforge.net/apps/mediawiki/amos/index.php?title=Bank2contig
 *
 * The TIGR file contains the contigs as well as information about the reads
 * aligned to each contig.
 *
 * The library file is a text file which lists the name of each mate pair
 * library along with its size (min/max length for reads in the library).
 * It also lists each mate pair and the library the mate pair comes from.
 * The library file is used to build the links between contigs.
 *
 * The final input is a fasta file containing shortened versions of all of
 * the original reads. The shortened reads are the subsequences
 * used with bowtie to align the reads to the contigs.
 */
public class AssembleScaffolds extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      AssembleScaffolds.class);
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    definitions.putAll(super.createParameterDefinitions());

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

    String outputPath = (String) stage_options.get("outputpath");
    File outputPathFile = new File(outputPath);
    if (outputPathFile.exists()) {
      sLogger.warn(
          "Outputpath: " + outputPath + " exists and will be deleted.");
      FileUtils.deleteDirectory(outputPathFile);
    }

    BuildBambusInput bambusInputStage = new BuildBambusInput();
    // Make a shallow copy of the stage options required by the stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            bambusInputStage.getParameterDefinitions().values());

    // Set the directory for the bambus inputs to be a subdirectory
    // of the output directory.
    String bambusOutputDir = FilenameUtils.concat(
        (String)stage_options.get("outputpath"), "bambus-input");
    stageOptions.put("outputpath", bambusOutputDir);
    bambusInputStage.setParameters(stageOptions);
    bambusInputStage.runJob();

    String amosPath = (String) stage_options.get("amos_path");
    String bankName = "bank.bnk";
    String bankPath = FilenameUtils.concat(outputPath, bankName);

    sLogger.info("Load the data into amos.");
    ArrayList<String> loadCommand = new ArrayList<String>();
    loadCommand.add(amosPath + "/toAmos_new");
    loadCommand.add("-s");
    loadCommand.add(bambusInputStage.getFastaOutputFile().getPath());
    loadCommand.add("-m");
    loadCommand.add(bambusInputStage.getLibraryOutputFile().getPath());
    loadCommand.add("-c");
    loadCommand.add(bambusInputStage.getContigOutputFile().getPath());
    loadCommand.add("-b");
    loadCommand.add(bankPath);

    if (ShellUtil.execute(loadCommand, null, "toAmos_new:", sLogger) != 0) {
      sLogger.fatal(
          "Failed to load the bambus input into the amos bank",
          new RuntimeException("Failed to load bambus input into amos bank."));
      System.exit(-1);
    }

    sLogger.info("Executing bambus.");
    // TODO(jeremy@lewi.us): We should write all bambus output to a
    // subdirectory of the outputpath.

    // It looks like goBambus2 can't take the path to the bank. The script
    // needs to be executed from the directory containing the bank.
    ArrayList<String> bambusCommand = new ArrayList<String>();
    bambusCommand.add(FilenameUtils.concat(amosPath, "goBambus2"));
    bambusCommand.add(bankName);
    bambusCommand.add("bambus_output");
    bambusCommand.add("clk");
    bambusCommand.add("bundle");
    bambusCommand.add("reps,\"-noPathRepeats\"");

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
    String outputPrefix = "scaffolds";
    ArrayList<String> orientCommand = new ArrayList<String>();
    orientCommand.add(FilenameUtils.concat(amosPath, "OrientContigs"));
    orientCommand.add("-b");
    orientCommand.add(bankPath);
    orientCommand.add("-maxOverlap");
    // TODO(jeremy@lewi.us): Max overlap should be a parameter.
    orientCommand.add("500");
    orientCommand.add("-redundancy");
    orientCommand.add("0");

    if (ShellUtil.execute(
        orientCommand, outputPath, "OrientContigs:", sLogger) != 0) {
        sLogger.fatal(
            "Bambus OrientContigs failed.",
            new RuntimeException("Bambus OrientContigs failed."));
        System.exit(-1);
    }

    // Run the bank2fasta stage to output the sequence of the contigs.
    // We do this manually because goBambus2 runs it with the -d option which
    // causes extra information to be printed as part of the contig id.
    ArrayList<String> bambus2Fasta = new ArrayList<String>();
    bambus2Fasta.add(FilenameUtils.concat(amosPath, "bank2fasta"));
    bambus2Fasta.add("-b");
    bambus2Fasta.add(bankName);
        //bambusOutput.add("printscaff");

    File fastaContigFile =
        new File(outputPath, outputPrefix + ".contigs.fasta");
    sLogger.info(
        "Writing scafold contig sequences to:" + fastaContigFile.getPath());
    PrintStream fastaStream = new PrintStream(
        new File(outputPath, outputPrefix + ".contigs.fasta"));

    if (ShellUtil.executeAndRedirect(
        bambus2Fasta, outputPath, "bank2fasta:", sLogger, fastaStream) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("Bambus failed."));
      System.exit(-1);
    }

    if (fastaStream.checkError()) {
      sLogger.fatal(
          "There was an error outputing the sequences for the scaffolds.",
          new RuntimeException("Error bank2fasta."));
      System.exit(-1);
    }

    ArrayList<String> bambusPrint = new ArrayList<String>();
    bambusPrint.add(FilenameUtils.concat(amosPath, "OutputScaffolds"));
    bambusPrint.add("-b");
    bambusPrint.add(bankName);

    String contigFile =
        FilenameUtils.concat(outputPath, outputPrefix + ".scaffolds.fasta");
    PrintStream contigStream = new PrintStream(contigFile);
    sLogger.info("Writing scaffold sequences to:" + contigFile);
    if (ShellUtil.executeAndRedirect(
        bambusPrint, outputPath, "OutputScaffolds:", sLogger, contigStream)
        != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("OutputScaffolds failed."));
      System.exit(-1);
    }
    contigStream.close();

    // Linearize the scaffolds.
    ArrayList<String> bambusLinearize = new ArrayList<String>();
    bambusLinearize.add(FilenameUtils.concat(amosPath, "Linearize"));
    bambusLinearize.add("-b");
    bambusLinearize.add(bankName);

    if (ShellUtil.execute(
        bambusLinearize, outputPath, "Linearize:", sLogger) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("Linearize failed."));
      System.exit(-1);
    }

    // Rerun output scaffolds.
    String linearFile =
        FilenameUtils.concat(
            outputPath, outputPrefix + ".scaffolds.linear.fasta");
    PrintStream linearStream = new PrintStream(linearFile);
    ArrayList<String> bambusPrintLinear = new ArrayList<String>();
    bambusPrintLinear.add(FilenameUtils.concat(amosPath, "OutputScaffolds"));
    bambusPrintLinear.add("-b");
    bambusPrintLinear.add(bankName);

    sLogger.info("Writing linearized scaffold sequences to:" + linearFile);
    if (ShellUtil.executeAndRedirect(
        bambusPrintLinear, outputPath, "OutputScaffolds:", sLogger,
        linearStream) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("OutputScaffolds failed."));
      System.exit(-1);
    }
    linearStream.close();

    // Run the stage.
    // TODO(jeremy@lewi.us): Process the data and generate a report.
    return null;
  }

  public static void main(String[] args) throws Exception {
    AssembleScaffolds stage = new AssembleScaffolds();
    int res = stage.run(args);
    System.exit(res);
  }
}
