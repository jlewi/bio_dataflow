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
      sLogger.fatal("There was a problem writing the html report. " +
          "Exception: " + exception.getMessage(), exception);
      System.exit(-1);
    }
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

    HashMap<String, SequenceSize> contigSizes = getSequenceSizes(contigFile);
    HashMap<String, SequenceSize> linearSizes = getSequenceSizes(linearFile);
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
