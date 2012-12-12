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

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;
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

  /**
   * Class for storing the size of a sequence.
   */
  static private class SequenceSize {
    final public int ungapped;
    final public int gapped;
    public SequenceSize(int gapped, int ungapped) {
      this.gapped = gapped;
      this.ungapped = ungapped;
    }
  }

  /**
   * Get the length of the sequence.
   *
   * If the member variable ungapped is true then we count gap characters
   * in the sequence. Otherwise we don't.
   *
   * @param fastaSeq
   * @return
   */
  private SequenceSize getFastaStringLength(String fastaSeq) {
     int ungapped = fastaSeq.replaceAll("N", "").replaceAll(
         "n", "").replaceAll("-", "").length();
     return new SequenceSize(fastaSeq.length(), ungapped);
  }

  /**
   * Read the fasta file and print out the length of each fasta sequence.
   * @param inputFile
   * @return: HashMap containing the size for each sequence.
   * @throws Exception
   */
  private HashMap<String, SequenceSize> getSequenceSizes(String inputFile)
      throws Exception {
     FastaFileReader reader = new FastaFileReader(inputFile);

     HashMap<String, SequenceSize> sizes = new HashMap<String, SequenceSize>();
     while (reader.hasNext()) {
       FastaRecord record = reader.next();
       if (sizes.containsKey(record.getId().toString())) {
         sLogger.fatal(
             "Duplicate read id:" + record.getId(),
             new RuntimeException("Duplicate ID"));
         System.exit(-1);
       }
       sizes.put(
           record.getId().toString(),
           getFastaStringLength(record.getRead().toString()));
     }

     return sizes;
  }

  private void writeReport(
      String reportFile, HashMap<String, SequenceSize> contigSizes,
      HashMap<String, SequenceSize> linearSizes) {
    // Currently the report has to be on a regular filesystem (i.e. non HDFS).
    // Since the rest of scaffolding has the same requirement this isn't
    // a burden.
    sLogger.info("Wrote HTML report to: " + reportFile);

    try {
      FileWriter fileWriter = new FileWriter(reportFile);
      BufferedWriter writer = new BufferedWriter(fileWriter);

      //writer.create(schema, outputStream);
      writer.append("<html><body>");

      writer.append("<h1>Size of scaffolds</h1>");
      writer.append("<table border=1>");
      writer.append("<tr><td>Scaffold Id</td>");
      writer.append("<td>Size with gaps.</td>");
      writer.append("<td>UngappedSize</td></tr>");
      for (String id : contigSizes.keySet()) {
        SequenceSize size = contigSizes.get(id);
        writer.append(String.format("<td>%s</td>", id));
        writer.append(String.format("<td>%d</td>", size.gapped));
        writer.append(String.format("<td>%d</td>", size.ungapped));

      }
      writer.append("</table>");

      writer.append("<h1>Size of Linear Scaffolds</h1>");
      writer.append("<table border=1>");
      writer.append("<tr><td>Scaffold Id</td>");
      writer.append("<td>Size with gaps.</td>");
      writer.append("<td>UngappedSize</td></tr>");
      for (String id : linearSizes.keySet()) {
        SequenceSize size = linearSizes.get(id);
        writer.append(String.format("<td>%s</td>", id));
        writer.append(String.format("<td>%d</td>", size.gapped));
        writer.append(String.format("<td>%d</td>", size.ungapped));
      }
      writer.append("</table>");


      writer.append("</body></html>");
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the html report. " +
          "Exception: " + exception.getMessage());
    }
  }

  private String getBankName() {
    return "bank.bnk";
  }

  private String getBankPath() {
    String outputPath = (String) stage_options.get("outputpath");
    return FilenameUtils.concat(outputPath, getBankName());
  }

  private String getOutputPrefix() {
    return "scaffolds";
  }

  private void runLoadIntoAmos(
      String fastaFile, String libraryFile, String contigOutputFile) {
    String amosPath = (String) stage_options.get("amos_path");
    sLogger.info("Load the data into amos.");
    ArrayList<String> loadCommand = new ArrayList<String>();
    loadCommand.add(amosPath + "/toAmos_new");
    loadCommand.add("-s");
    loadCommand.add(fastaFile);
    loadCommand.add("-m");
    loadCommand.add(libraryFile);
    loadCommand.add("-c");
    loadCommand.add(contigOutputFile);
    loadCommand.add("-b");
    loadCommand.add(getBankPath());

    if (ShellUtil.execute(loadCommand, null, "toAmos_new:", sLogger) != 0) {
      sLogger.fatal(
          "Failed to load the bambus input into the amos bank",
          new RuntimeException("Failed to load bambus input into amos bank."));
      System.exit(-1);
    }
  }

  /**
   * Bank2fasta prints out the contig sequences.
   */
  private void runBank2Fasta() {
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    // Run the bank2fasta stage to output the sequence of the contigs.
    // We do this manually because goBambus2 runs it with the -d option which
    // causes extra information to be printed as part of the contig id.
    ArrayList<String> bambus2Fasta = new ArrayList<String>();
    bambus2Fasta.add(FilenameUtils.concat(amosPath, "bank2fasta"));
    // Add the -d option to include information about lengths and coverages
    // on the id line.
    bambus2Fasta.add("-b");
    bambus2Fasta.add(getBankName());

    File fastaContigFile =
        new File(outputPath, getOutputPrefix() + ".contigs.fasta");
    sLogger.info(
        "Writing scafold contig sequences to:" + fastaContigFile.getPath());
    PrintStream fastaStream = null;
    try {
      fastaStream = new PrintStream(fastaContigFile.getPath());
    } catch (FileNotFoundException e) {
      sLogger.fatal("Could not write scaffold contig sequences.", e);
      System.exit(-1);
    }

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
  }

  private void runNonlinearOutputResults() {
    String amosPath = (String) stage_options.get("amos_path");
    String nonlinearPrefix = getOutputPrefix() + ".scaffolds.nonlinear";
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> bambusPrint = new ArrayList<String>();
    bambusPrint.add(FilenameUtils.concat(amosPath, "OutputResults"));
    bambusPrint.add("-b");
    bambusPrint.add(getBankName());
    bambusPrint.add("-p");
    bambusPrint.add(nonlinearPrefix);
//    String contigFile =
//        FilenameUtils.concat(outputPath, outputPrefix + ".scaffolds");
//    PrintStream contigStream = new PrintStream(contigFile);

    sLogger.info(
        "Writing non-linearized scaffolds with prefix :" + nonlinearPrefix);
    if (ShellUtil.execute(bambusPrint, outputPath, "OutputScaffolds:", sLogger)
        != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("OutputScaffolds failed."));
      System.exit(-1);
    }
  }

  private void runLinearize() {
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    // Linearize the scaffolds.
    ArrayList<String> bambusLinearize = new ArrayList<String>();
    bambusLinearize.add(FilenameUtils.concat(amosPath, "Linearize"));
    bambusLinearize.add("-b");
    bambusLinearize.add(getBankPath());

    if (ShellUtil.execute(
        bambusLinearize, outputPath, "Linearize:", sLogger) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("Linearize failed."));
      System.exit(-1);
    }
  }

  private void runOrientContigs() {
    // We manually run the oriengt, 2fasta, and printscaff stages rather
    // than using the goBambus binary because we ran into issues with
    // the goBambus script and the parsing of the arguments for the orient
    // stage.
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> orientCommand = new ArrayList<String>();
    orientCommand.add(FilenameUtils.concat(amosPath, "OrientContigs"));
    orientCommand.add("-b");
    orientCommand.add(getBankPath());
    orientCommand.add("-maxOverlap");
    // TODO(jeremy@lewi.us): Max overlap should be a parameter.
    orientCommand.add("500");
    orientCommand.add("-redundancy");
    orientCommand.add("0");
    // Add the -all information so that we print a file containing degenerate
    // contigs.
    orientCommand.add("-all");

    if (ShellUtil.execute(
        orientCommand, outputPath, "OrientContigs:", sLogger) != 0) {
        sLogger.fatal(
            "Bambus OrientContigs failed.",
            new RuntimeException("Bambus OrientContigs failed."));
        System.exit(-1);
    }
  }

  private void runGoBambus() {
    sLogger.info("Executing bambus.");
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    // It looks like goBambus2 can't take the path to the bank. The script
    // needs to be executed from the directory containing the bank.
    ArrayList<String> bambusCommand = new ArrayList<String>();
    bambusCommand.add(FilenameUtils.concat(amosPath, "goBambus2"));
    bambusCommand.add(getBankName());
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
  }

  private void runLinearOutputResults(){
    String linearPrefix = getOutputPrefix() + ".scaffolds.linear";
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> bambusPrintLinear = new ArrayList<String>();
    bambusPrintLinear.add(FilenameUtils.concat(amosPath, "OutputResults"));
    bambusPrintLinear.add("-b");
    bambusPrintLinear.add(getBankPath());
    bambusPrintLinear.add("-prefix");
    bambusPrintLinear.add(linearPrefix);

    //String linearFile =
    //    FilenameUtils.concat(
    //        outputPath, linearPrefix + ".fasta");

    //sLogger.info("Writing linearized scaffold sequences to:" + linearFile);
//    ShellUtil.executeAndRedirect(
//        bambusPrintLinear, outputPath, "OutputResults:", sLogger,
//        linearStream) != 0
    if (ShellUtil.execute(
        bambusPrintLinear, outputPath, "OutputResults:", sLogger) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("OutputResults failed."));
      System.exit(-1);
    }
//    linearStream.close();
  }

  /**
   * Output the fasta sequences of the scaffolds.
   *
   * @param scaffoldFile
   */
  private void runOutputScaffolds(String scaffoldFile) {
    //String linearPrefix = getOutputPrefix() + ".scaffolds.linear";
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> bambusPrintLinear = new ArrayList<String>();
    bambusPrintLinear.add(FilenameUtils.concat(amosPath, "OutputScaffolds"));
    bambusPrintLinear.add("-b");
    bambusPrintLinear.add(getBankPath());

    //String linearFile =
    //    FilenameUtils.concat(
    //        outputPath, linearPrefix + ".fasta");

    //sLogger.info("Writing linearized scaffold sequences to:" + linearFile);
    PrintStream scaffoldStream = null;
    try {
      scaffoldStream = new PrintStream(new File(scaffoldFile));
    } catch (FileNotFoundException e) {
      sLogger.fatal(
          "Could not create file:" + scaffoldFile, e);
      System.exit(-1);
    }

    if (ShellUtil.executeAndRedirect(
        bambusPrintLinear, outputPath, "OutputResults:", sLogger,
        scaffoldStream) != 0) {
      sLogger.fatal(
          "Outputting scaffolds failed.",
          new RuntimeException("OutputScaffolds failed."));
      System.exit(-1);
    }

    scaffoldStream.close();
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

    runLoadIntoAmos(
        bambusInputStage.getFastaOutputFile().getPath(),
        bambusInputStage.getLibraryOutputFile().getPath(),
        bambusInputStage.getContigOutputFile().getPath());

    runGoBambus();
    // Make a copy of the bambus log because when we rerun bambus below
    // the log will get overwritten.
    // TODO(jeremy@lewi.us): Do we still need this?
    FileUtils.copyFile(
        new File(outputPath, "bambus2.log"),
        new File(outputPath, "bambus2.log.initial_stages"));


    runOrientContigs();
    runNonlinearOutputResults();
    runBank2Fasta();

    String nonLinearScaffoldFile = FilenameUtils.concat(
        outputPath, getOutputPrefix() + ".scaffolds.nonlinear.fasta");
    runOutputScaffolds(nonLinearScaffoldFile);

    runLinearize();
    runLinearOutputResults();
    String linearScaffoldFile = FilenameUtils.concat(
        outputPath, getOutputPrefix() + ".scaffolds.linear.fasta");
    runOutputScaffolds(linearScaffoldFile);

    HashMap<String, SequenceSize> contigSizes = getSequenceSizes(
        nonLinearScaffoldFile);
    HashMap<String, SequenceSize> linearSizes = getSequenceSizes(
        linearScaffoldFile);
    String reportFile = FilenameUtils.concat(
        outputPath, "scaffold_report.html");
    writeReport(reportFile, contigSizes, linearSizes);
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
