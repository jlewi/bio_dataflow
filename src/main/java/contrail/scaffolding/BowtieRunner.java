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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * This class is used to run the bowtie short read aligner.
 */
public class BowtieRunner {
  private static final Logger sLogger = Logger.getLogger(BowtieRunner.class);
  private String bowtiePath;
  private String bowtieBuildPath;
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
    String command = String.format(
        "%s %s %s", bowtieBuildPath, fileList,
        new File(outDir, outBase).getAbsolutePath());
    boolean success = false;

    File outDirFile = new File(outDir);
    // TODO(jeremy@lewi.us): What should we do if the index directory
    // already exists?
    if (!outDirFile.exists()) {
      outDirFile.mkdir();
    } else {
      sLogger.warn(
          "Directory to store the bowtie index already exists: " + outDir);
    }

    try {
      sLogger.info("Running bowtie to build index:" + command);
      Process p = Runtime.getRuntime().exec(command);

      BufferedReader stdInput = new BufferedReader(
          new InputStreamReader(p.getInputStream()));

      BufferedReader stdError = new BufferedReader(
          new InputStreamReader(p.getErrorStream()));

      p.waitFor();
      String line;
      while ((line = stdInput.readLine()) != null) {
        sLogger.info("bowtie-build: " + line);
      }
      while ((line = stdError.readLine()) != null) {
        sLogger.error("bowtie-build: " + line);
      }

      sLogger.info("bowtie-build: Exit Value: " + p.exitValue());
      if (p.exitValue() == 0) {
        success = true;
      } else {
        sLogger.error("bowtie-build failed command was: " + command);
      }

    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem executing bowtie. The command was:\n" +
          command + "\n. The Exception was:\n" + e.getMessage());
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Bowtie execution was interupted. The command was:\n" +
          command + "\n. The Exception was:\n" + e.getMessage());
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
   * Run the bowtie aligner.
   * @param indexDir: The base directory for the bowtie index.
   * @param indexBase: The prefix for the bowtie index files.
   * @param fastqFiles: Collection of the fastq files to align.
   *   The reads should already have been truncated to a suitable length.
   * @param outDir: The directory where the bowtie output should be written.
   * @return: An instance of AlignResult containing the results.
   */
  public AlignResult alignReads(
      String indexDir, String indexBase, Collection<String> fastqFiles,
      String outDir) {
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

      if (outputs.contains(outFile)) {
        // TODO(jeremy@lewi.us). We could handle this just by appending
        // a suffix to make the filename unique.
        String message =
            "Error output: " + outFile + " would overwrite the output for a " +
            "previous input. This can happen if two input files have the " +
            "same base name.";
        sLogger.error(message);
        throw new RuntimeException(message);
      }
      outputs.add(outFile);
      result.outputs.put(fastqFile, outFile);
      String command = String.format(
          "%s -v 1 -M 2  %s %s %s", bowtiePath, bowtieIndex, fastqFile,
          outFile);

      try {
        sLogger.info("Running bowtie to align the reads:" + command);
        Process p = Runtime.getRuntime().exec(command);

        BufferedReader stdInput = new BufferedReader(
            new InputStreamReader(p.getInputStream()));

        BufferedReader stdError = new BufferedReader(
            new InputStreamReader(p.getErrorStream()));

        p.waitFor();
        String line;
        while ((line = stdInput.readLine()) != null) {
          sLogger.info("bowtie: " + line);
        }
        while ((line = stdError.readLine()) != null) {
          sLogger.error("bowtie: " + line);
        }

        sLogger.info("bowtie: Exit Value: " + p.exitValue());
        if (p.exitValue() == 0) {
          result.success = true;
        } else {
          sLogger.error("bowtie-build failed command was: " + command);
        }

      } catch (IOException e) {
        throw new RuntimeException(
            "There was a problem executing bowtie. The command was:\n" +
            command + "\n. The Exception was:\n" + e.getMessage());
      } catch (InterruptedException e) {
        throw new RuntimeException(
            "Bowtie execution was interupted. The command was:\n" +
            command + "\n. The Exception was:\n" + e.getMessage());
      }
    }

    return result;
  }
}
