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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.stages.StageBase.InvalidParameter;
import contrail.util.ShellUtil;

/**
 * Assembly the scaffolds by running bambus.
 *
 * Bambus takes as input 3 files which can be built using BuildBambusInput.
 *
 * The first file is the contig file in
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
 *
 */
public class RunBambus extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      RunBambus.class);
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    definitions.putAll(super.createParameterDefinitions());

    ParameterDefinition amosPath =
        new ParameterDefinition(
            "amos_path", "The directory where the amos tools are installed.",
            String.class, null);

    ParameterDefinition maxOverlap =
        new ParameterDefinition(
            "max_overlap", "Bambus parameter maxoverlap.",
            Integer.class, new Integer(500));

    // Graph reduction is currently off because we were running into
    // problems with the graph reduction code for the staph dataset.
    ParameterDefinition noReduce =
        new ParameterDefinition(
            "noreduce", "Turn off graph simplification in orient contigs.",
            Boolean.class, true);

    ParameterDefinition startStep =
        new ParameterDefinition(
            "start_step",
            "Which step in the scaffolding pipeline to start at. By default " +
            "all steps are run. This option is mostly for debugging.",
            String.class, "load_into_amos");

    ParameterDefinition stopStep =
        new ParameterDefinition(
            "stop_step",
            "The last step in the scaffolding pipeline to run. By default " +
            "all steps are run. This option is mostly for debugging.",
            String.class, "write_report");

    // The data to load into Amos which is produced by BuildBambusInput.
    // TODO(jlewi): We should an option to allow BuildBambusInput to be run.
    ParameterDefinition fastaFile =
        new ParameterDefinition(
            "fasta_file",
            "A single fasta file on the local filesystem containing all the " +
            "reads .",
            String.class, null);

    ParameterDefinition tigrFile =
        new ParameterDefinition(
            "tigr_file",
            "The tigr file containing the contigs and information about the " +
            " aligned reads. This should be on the local filesystem.",
            String.class, null);

    ParameterDefinition matesFile =
        new ParameterDefinition(
            "mates_file",
            "The mates file containing information about the mate pairs. " +
            "This should be on the local filesystem.",
            String.class, null);

    ParameterDefinition output = new ParameterDefinition(
        "outputpath", "The local directory where the output should be " +
        "written to.", String.class, null);

    for (ParameterDefinition def: new ParameterDefinition[] {
            amosPath, maxOverlap, noReduce, startStep, stopStep, fastaFile,
            tigrFile, matesFile, output}) {
      definitions.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(definitions);
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> invalid = super.validateParameters();

    invalid.addAll(this.checkParameterIsNonEmptyString(Arrays.asList(
        "outputpath")));

    invalid.addAll(this.checkParameterIsExistingLocalFile(Arrays.asList(
        "amos_path", "fasta_file", "mates_file", "tigr_file")));

    String startStep = (String) stage_options.get("start_step");
    startStep = startStep.toUpperCase();

    HashSet<String> validSteps = new HashSet<String>();
    for (ScaffoldingSteps step : ScaffoldingSteps.values()) {
      validSteps.add(step.name());
    }

    if (!validSteps.contains(startStep)) {
      InvalidParameter parameter = new InvalidParameter(
          "step_path", String.format(
          "The value of --step_path is invalid. Allowed values are: %s",
          StringUtils.join(ScaffoldingSteps.values(), ",")));
      invalid.add(parameter);
    }

    return invalid;
  }

  /**
   * Class for storing the size of a sequence.
   */
  static private class SequenceSize implements Comparable<SequenceSize> {
    final public int ungapped;
    final public int gapped;
    final public String id;
    public SequenceSize(String id, int gapped, int ungapped) {
      this.id = id;
      this.gapped = gapped;
      this.ungapped = ungapped;
    }

    // Implement the compare and equals method so we can sort by size.
    @Override
    public int compareTo(SequenceSize other) {
      if (this.gapped < other.gapped) {
        return -1;
      } else if (this.gapped == other.gapped) {
        return 0;
      } else {
        return 1;
      }
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
  private SequenceSize getFastaStringLength(FastaRecord record) {
     String fastaSeq = record.getRead().toString();
     int ungapped = fastaSeq.replaceAll("N", "").replaceAll(
         "n", "").replaceAll("-", "").length();
     return new SequenceSize(
         record.getId().toString(), fastaSeq.length(), ungapped);
  }

  /**
   * Read the fasta file and print out the length of each fasta sequence.
   * @param inputFile
   * @return: HashMap containing the size for each sequence.
   * @throws Exception
   */
  private ArrayList<SequenceSize> getSequenceSizes(String inputFile) {
     FastaFileReader reader = new FastaFileReader(inputFile);
     HashSet<String> sequences = new HashSet<String>();
     ArrayList<SequenceSize> sizes = new ArrayList<SequenceSize>();
     while (reader.hasNext()) {
       FastaRecord record = reader.next();
       if (sequences.contains(record.getId().toString())) {
         sLogger.fatal(
             "Duplicate read id:" + record.getId(),
             new RuntimeException("Duplicate ID"));
         System.exit(-1);
       }
       sequences.add(record.getId().toString());
       sizes.add(getFastaStringLength(record));
     }

     return sizes;
  }

  private void writeSequenceSizes(
      BufferedWriter writer, String header, ArrayList<SequenceSize> sizes) {
    // Sort by size in descending order.
    Collections.sort(sizes);
    Collections.reverse(sizes);
    int gapped = 0;
    int ungapped = 0;
    int numContigs = 0;
    try {
      writer.append("<h1>" + header + "</h1>");
      writer.append("<table border=1>");
      writer.append("<tr><td>Scaffold Id</td>");
      writer.append("<td>Size with gaps.</td>");
      writer.append("<td>UngappedSize</td>");
      writer.append("<td>Cumulative Num of contigs</td>");
      writer.append("<td>Cumulative Gapped Size</td>");
      writer.append("<td>Cumulative Ungapped Size</td></tr>");
      for (SequenceSize sequence : sizes) {
        numContigs += 1;
        gapped += sequence.gapped;
        ungapped += sequence.ungapped;
        writer.append("<tr>");
        writer.append(String.format("<td>%s</td>", sequence.id));
        writer.append(String.format("<td>%d</td>", sequence.gapped));
        writer.append(String.format("<td>%d</td>", sequence.ungapped));
        writer.append(String.format("<td>%d</td>", numContigs));
        writer.append(String.format("<td>%d</td>", gapped));
        writer.append(String.format("<td>%d</td>", ungapped));
        writer.append("</tr>");
      }
      // Totals.
      writer.append("<tr>");
      writer.append(String.format("<td>Total</td>"));
      writer.append(String.format("<td>%d</td>", gapped));
      writer.append(String.format("<td>%d</td>", ungapped));
      writer.append("</tr>");

      writer.append("</table>");
    } catch (IOException exception) {
      sLogger.fatal("There was a problem writing the html report. " +
          "Exception: " + exception.getMessage(), exception);
      System.exit(-1);
    }
  }

  private void writeReport(
      String reportFile, ArrayList<SequenceSize> contigSizes,
      ArrayList<SequenceSize> linearSizes) {
    // Currently the report has to be on a regular filesystem (i.e. non HDFS).
    // Since the rest of scaffolding has the same requirement this isn't
    // a burden.
    sLogger.info("Wrote HTML report to: " + reportFile);

    try {
      FileWriter fileWriter = new FileWriter(reportFile);
      BufferedWriter writer = new BufferedWriter(fileWriter);

      writer.append("<html><body>");

      writeSequenceSizes(writer, "Size of scaffolds", contigSizes);
      writeSequenceSizes(writer, "Size of Linearized scaffolds", linearSizes);

      writer.append("</table>");


      writer.append("</body></html>");
      writer.close();
    } catch (IOException exception) {
      sLogger.fatal("There was a problem writing the html report. " +
          "Exception: " + exception.getMessage(), exception);
      System.exit(-1);
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
    // TODO(jeremy@lewi.us) We use a function so that we could potentially
    // change this in the future to a parameter.
    return "";
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
        new File(outputPath, getOutputPrefix() + "contigs.fasta");
    sLogger.info(
        "Writing scaffold contig sequences to:" + fastaContigFile.getPath());
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

    // Make a copy of the bambus log because when we rerun bambus in
    // later stage the log will get overwritten.
    // TODO(jeremy@lewi.us): Do we still need this?
    try {
      FileUtils.copyFile(
          new File(outputPath, "bambus2.log"),
          new File(outputPath, "bambus2.log.initial_stages"));
    } catch (IOException e) {
      sLogger.fatal("Failed to copy files.", e);
      System.exit(-1);
    }
  }

  private void runLinearOutputResults(){
    String linearPrefix = getOutputPrefix() + "scaffolds.linear";
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> bambusPrintLinear = new ArrayList<String>();
    bambusPrintLinear.add(FilenameUtils.concat(amosPath, "OutputResults"));
    bambusPrintLinear.add("-b");
    bambusPrintLinear.add(getBankPath());
    bambusPrintLinear.add("-prefix");
    bambusPrintLinear.add(linearPrefix);

    if (ShellUtil.execute(
        bambusPrintLinear, outputPath, "OutputResults:", sLogger) != 0) {
      sLogger.fatal(
          "Bambus failed.",
          new RuntimeException("OutputResults failed."));
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

  private void runLoadIntoAmos(
      String fastaFile, String libraryFile, String contigOutputFile) {
    if (new File(getBankPath()).exists()) {
      sLogger.info(String.format("Deleting existing bank: %s", getBankPath()));
      try {
        FileUtils.deleteDirectory(new File(getBankPath()));
      } catch (IOException e) {
        sLogger.fatal(
            String.format("Error deleting bank: %s", getBankPath()),
            e);
        System.exit(-1);
      }
    }

    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");

    File outDir = new File(outputPath);
    if (!outDir.exists()) {
      if (!outDir.mkdirs()) {
        sLogger.fatal("Could not create directory:" + outputPath);
        System.exit(-1);
      }
    }

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

    if (ShellUtil.execute(loadCommand, outputPath, "toAmos_new:", sLogger)
        != 0) {
      sLogger.fatal(
          "Failed to load the bambus input into the amos bank",
          new RuntimeException("Failed to load bambus input into amos bank."));
      System.exit(-1);
    }
  }

  private void runNonlinearOutputResults() {
    String amosPath = (String) stage_options.get("amos_path");
    String nonlinearPrefix = getOutputPrefix() + "scaffolds.nonlinear";
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> bambusPrint = new ArrayList<String>();
    bambusPrint.add(FilenameUtils.concat(amosPath, "OutputResults"));
    bambusPrint.add("-b");
    bambusPrint.add(getBankName());
    bambusPrint.add("-p");
    bambusPrint.add(nonlinearPrefix);

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

  private void runOrientContigs() {
    // We manually run the oriengt, 2fasta, and printscaff stages rather
    // than using the goBambus binary because we ran into issues with
    // the goBambus script and the parsing of the arguments for the orient
    // stage.
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> orientCommand = new ArrayList<String>();
    orientCommand.add(FilenameUtils.concat(amosPath, "OrientContigs"));

    Integer maxOverlap = (Integer)stage_options.get("max_overlap");
    Boolean noReduce = (Boolean)stage_options.get("noreduce");
    // The -all option means that we will output a scaffold even if it is
    // degenerate; i.e consists of a single scaffold.
    orientCommand.add("-all");
    orientCommand.add("-b");
    orientCommand.add(getBankPath());
    orientCommand.add("-maxOverlap");
    orientCommand.add(maxOverlap.toString());
    orientCommand.add("-redundancy");
    orientCommand.add("0");
    if (noReduce) {
      orientCommand.add("-noreduce");
    }

    if (ShellUtil.execute(
        orientCommand, outputPath, "OrientContigs:", sLogger) != 0) {
        sLogger.fatal(
            "Bambus OrientContigs failed.",
            new RuntimeException("Bambus OrientContigs failed."));
        System.exit(-1);
    }
  }

  /**
   * Output the fasta sequences of the scaffolds.
   *
   * @param scaffoldFile
   */
  private void runOutputScaffolds(String scaffoldFile) {
    String amosPath = (String) stage_options.get("amos_path");
    String outputPath = (String) stage_options.get("outputpath");
    ArrayList<String> bambusPrintLinear = new ArrayList<String>();
    bambusPrintLinear.add(FilenameUtils.concat(amosPath, "OutputScaffolds"));
    bambusPrintLinear.add("-b");
    bambusPrintLinear.add(getBankPath());

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

  // Different steps in scaffolding.
  private static enum ScaffoldingSteps {
    LOAD_INTO_AMOS,
    RUN_AMOS,
    RUN_BAMBUS,
    RUN_ORIENT_CONTIGS,
    NONLINEAR_OUTPUT,
    BANK_TO_FASTA,
    OUTPUT_NONLINEAR_SCAFFOLDS,
    LINEARIZE,
    LINEAR_OUTPUT,
    OUTPUT_LINEAR_SCAFFOLDS,
    WRITE_REPORT
  }

  private void deleteExistingDirs() {
    // Delete any output directories that already exist.
    // Delete the posix, local outputpath.
    String outputPath = (String) stage_options.get("outputpath");
    File outDir = new File(outputPath);
    if (outDir.exists()) {
      try {
        FileUtils.deleteDirectory(outDir);
      } catch (IOException e) {
        sLogger.fatal(
            "There was a problem deleting the directory: " + outputPath, e);
        System.exit(-1);
      }
    }
  }

  @Override
  protected void stageMain() {
    String startPhase = (String) stage_options.get("start_step");
    String stopPhase = (String) stage_options.get("stop_step");
    ScaffoldingSteps startStep = ScaffoldingSteps.valueOf(
        startPhase.toUpperCase());
    ScaffoldingSteps stopStep = ScaffoldingSteps.valueOf(
        stopPhase.toUpperCase());

    String outputPath = (String) stage_options.get("outputpath");

    String nonLinearScaffoldFile = FilenameUtils.concat(
        outputPath, getOutputPrefix() + "scaffolds.nonlinear.fasta");

    String linearScaffoldFile = FilenameUtils.concat(
        outputPath, getOutputPrefix() + "scaffolds.linear.fasta");

    // Run all steps starting at start step.
    switch (startStep) {
      case LOAD_INTO_AMOS:
        deleteExistingDirs();
        runLoadIntoAmos(
            (String) stage_options.get("fasta_file"),
            (String) stage_options.get("mates_file"),
            (String) stage_options.get("tigr_file"));
        if (stopStep == ScaffoldingSteps.LOAD_INTO_AMOS) {
          break;
        }
      case RUN_BAMBUS:
        runGoBambus();
        if (stopStep == ScaffoldingSteps.RUN_BAMBUS) {
          break;
        }
      case RUN_ORIENT_CONTIGS:
        runOrientContigs();
        if (stopStep == ScaffoldingSteps.RUN_ORIENT_CONTIGS) {
          break;
        }
      case NONLINEAR_OUTPUT:
        runNonlinearOutputResults();
        if (stopStep == ScaffoldingSteps.NONLINEAR_OUTPUT) {
          break;
        }
      case BANK_TO_FASTA:
        runBank2Fasta();
        if (stopStep == ScaffoldingSteps.BANK_TO_FASTA) {
          break;
        }
      case OUTPUT_NONLINEAR_SCAFFOLDS:
        runOutputScaffolds(nonLinearScaffoldFile);
        if (stopStep == ScaffoldingSteps.OUTPUT_NONLINEAR_SCAFFOLDS) {
          break;
        }
      case LINEARIZE:
        runLinearize();
        if (stopStep == ScaffoldingSteps.LINEARIZE) {
          break;
        }
      case LINEAR_OUTPUT:
        runLinearOutputResults();
        if (stopStep == ScaffoldingSteps.LINEAR_OUTPUT) {
          break;
        }
      case OUTPUT_LINEAR_SCAFFOLDS:
        runOutputScaffolds(linearScaffoldFile);
        if (stopStep == ScaffoldingSteps.OUTPUT_LINEAR_SCAFFOLDS) {
          break;
        }
      case WRITE_REPORT:
        ArrayList<SequenceSize> contigSizes = getSequenceSizes(
            nonLinearScaffoldFile);
        ArrayList<SequenceSize> linearSizes = getSequenceSizes(
            linearScaffoldFile);
        String reportFile = FilenameUtils.concat(
            outputPath, getOutputPrefix() + "scaffolds.report.html");
        writeReport(reportFile, contigSizes, linearSizes);
        if (stopStep == ScaffoldingSteps.WRITE_REPORT) {
          break;
        }
      default:
        sLogger.fatal(String.format(
            "%s is not a valid start phase.", startPhase));
        System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new RunBambus(), args);
    System.exit(res);
  }
}
