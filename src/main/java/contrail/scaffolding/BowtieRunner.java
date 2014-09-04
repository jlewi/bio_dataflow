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
// Author: Jeremy Lewi (jeremy@lewi.us), Serge Koren(sergekoren@gmail.com)
package contrail.scaffolding;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.util.ShellUtil;

/**
 * This class is used to run the bowtie short read aligner.
 */
public class BowtieRunner {
  private static final Logger sLogger = Logger.getLogger(BowtieRunner.class);
  private final String bowtiePath;
  private final String bowtieBuildPath;

  // The initial capacity for the array to store the alignments to each
  // reference contig.
  private static final int NUM_READS_PER_CTG = 200;

  /**
   * Construct the runner.
   * @param bowTiePath: Path to bowtie.
   * @param bowtieBuildPath: Path to bowtie-build the binary for building
   *   bowtie indexes.
   */
  public BowtieRunner(String bowtiePath, String bowtieBuildPath) {
    this.bowtiePath = bowtiePath;
    this.bowtieBuildPath = bowtieBuildPath;
  }

  /**
   * Build the bowtie index by invoking bambus.
   *
   * @param contigFiles
   * @param outDir: The directory where the index should be written.
   * @param outBase: The base name for the index files.
   * @return: True if the build was successful and false otherwise.
   */
  public boolean bowtieBuildIndex(
      Collection<String> contigFiles, String outDir, String outBase) {
    String fileList = StringUtils.join(contigFiles, ",");
    ArrayList<String> commandArgs = new ArrayList<String>();
    commandArgs.add(bowtieBuildPath);
    commandArgs.add(fileList);
    commandArgs.add(new File(outDir, outBase).getAbsolutePath());

    boolean success = false;

    File outDirFile = new File(outDir);
    // TODO(jeremy@lewi.us): What should we do if the index directory
    // already exists?
    if (!outDirFile.exists()) {
      try {
        FileUtils.forceMkdir(outDirFile);
      } catch (IOException e) {
        sLogger.fatal(
            "Could not create the output directory for the bowtie index:" +
            outDirFile.getPath(), e);
      }
    } else {
      sLogger.warn(
          "Directory to store the bowtie index already exists: " + outDir);
    }

    if (ShellUtil.execute(commandArgs, null, "bowtie", sLogger) == 0) {
      success = true;
    }
    return success;
  }

  /**
   * Class to contain the result of running bowtie to align the reads.
   */
  public static class AlignResult {
    public AlignResult() {
      success = false;
      outputs = new HashMap<String, String>();
    }
    boolean success;

    // This is a map from the names of the input fastq files to the name
    // of the output file containing the alignments.
    HashMap<String, String> outputs;
  }

  /**
   * Shorten the fastq entries in the file to the specified length output them.
   * @param inFile
   * @param outFile
   * @param readLength
   */
  private void shortenFastQFile(String inFile, File outFile, int readLength) {
    try {
      // The code assumes that fastaFile is a local file. So we create a
      // configuration which has "file:///" as the default filesystem URI.
      Configuration conf = new Configuration();
      URI localDefaultURI = new URI("file:///");
      FileSystem.setDefaultUri(conf, localDefaultURI);

      FileOutputStream out = new FileOutputStream(outFile);
      FastQFileReader reader = new FastQFileReader(inFile, conf);

      while (reader.hasNext()) {
        FastQRecord record = reader.next();

        // TODO(jeremy@lewi.us): We used to use Utils.safeReadId to change
        // the read ids. This is supposedly needed because bowtie cuts off
        // the "/" and sets  a special code. So to be consistent we use the
        // function safeReadId to convert readId's to a version that can be
        // safely processed by bowtie.
        // We need to make the read ids safe.
        // record.setId(Utils.safeReadId(record.getId().toString()));
        if (readLength < record.getRead().length()) {
          record.setRead(
              record.getRead().toString().subSequence(0, readLength));
          record.setQvalue(
              record.getQvalue().toString().subSequence(0, readLength));
        }
        FastUtil.writeFastQRecord(out, record);
      }
    } catch (Exception e) {
      sLogger.fatal("Could not write shortened reads." , e);
      System.exit(-1);
    }
  }

  /**
   * Run the bowtie aligner.
   * @param indexDir: The base directory for the bowtie index.
   * @param indexBase: The prefix for the bowtie index files.
   * @param fastqFiles: Collection of the fastq files to align.
   *   The reads should already have been truncated to a suitable length.
   * @param readLength: Reads are shortened to this length before aligning
   *   them. This is neccessary because bowtie can only handle short reads.
   * @param outDir: The directory where the bowtie output should be written.
   * @return: An instance of AlignResult containing the results.
   */
  public AlignResult alignReads(
      String indexDir, String indexBase, Collection<String> fastqFiles,
      String outDir, int readLength) {
    AlignResult result = new AlignResult();
    result.success = false;

    File outDirFile = new File(outDir);
    // TODO(jeremy@lewi.us): What should we do if the index directory
    // already exists?
    if (!outDirFile.exists()) {
      outDirFile.mkdir();
    } else {
      sLogger.warn(
          "Directory to store the bowtie alignments already exists: " + outDir);
    }

    String bowtieIndex = new File(indexDir, indexBase).getAbsolutePath();

    // We keep track of all outputs written so far to make sure we don't
    // overwrite an existing output.
    HashSet<String> outputs = new HashSet<String>();

    for (String fastqFile : fastqFiles) {
      sLogger.info("Aligning the reads in file: " + fastqFile);
      String baseName = FilenameUtils.getBaseName(fastqFile);
      String outFile = FilenameUtils.concat(outDir, baseName + ".bout");

      File shortFastq = null;

      try {
        shortFastq = File.createTempFile("temp", baseName + ".fastq");
      } catch (IOException e) {
        sLogger.fatal(
            "Couldn't create temporary file for shortened reads.", e);
      }
      shortenFastQFile(fastqFile, shortFastq, readLength);

      if (outputs.contains(outFile)) {
        // TODO(jeremy@lewi.us). We could handle this just by appending
        // a suffix to make the filename unique.
        String message =
            "Error output: " + outFile + " would overwrite the output for a " +
                "previous input. This can happen if two input files have the " +
                "same base name.";
        sLogger.fatal(message, new RuntimeException(message));
        System.exit(-1);
      }

      outputs.add(outFile);
      result.outputs.put(fastqFile, outFile);
      ArrayList<String> command = new ArrayList<String>();
      command.add(bowtiePath);
      // For a description of bowtie options see
      // http://bowtie-bio.sourceforge.net/manual.shtml#output.
      // The -v # option means alignments can have no more than # mismatches
      // and causes quality values to be ignored.
      command.add("-v");
      command.add("1");
      // The -M # means that if a read has more than # reportable alignments
      // then one alignment is reported at random.
      // command.add("-M");
      // command.add("2");
      // -m 1 means we suppress all alignments if there is more than 1
      // reportable alignment. We do this because if the genome is heterozygous
      // a read could align to both contigs and this could confuse the
      // scaffolder.
      command.add("-m");
      command.add("1");
      command.add(bowtieIndex);
      command.add(shortFastq.getPath());
      command.add(outFile);

      sLogger.info("Running bowtie to align the reads:" + command);
      if (ShellUtil.execute(command, null, "bowtie", sLogger) == 0) {
        result.success = true;
      }
      shortFastq.delete();
    }

    return result;
  }
}
