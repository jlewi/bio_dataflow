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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.util.FileHelper;

/**
 * A binary useful for testing the code for building the bambus input.
 *
 * This class needs to be run manually because you need to specify the
 * path to bowtie.
 *
 * TODO(jeremy@lewi.us): We could try to make this an automatic unittest
 * by searching the path for bowtie and bowtie-build so that the user
 * doesn't have to specify the manually.
 */
public class TestBuildBambusInput {
  private final ArrayList<File> dirsToDelete;

  public TestBuildBambusInput() {
    dirsToDelete = new ArrayList<File>();
  }

  private static class TestData {
    public TestData() {
      referenceFiles = new ArrayList<String>();
      readFiles = new ArrayList<String>();
    }
    // Files containing the sequences we want to align to.
    public ArrayList<String> referenceFiles;

    // The short reads to align to the reference.
    public ArrayList<String> readFiles;

    public String readsGlob;
    public String referenceGlob;
    public String libSizeFile;
  }

  private void writeFastQRecord(
      BufferedWriter out, String readId, String sequence) throws IOException {
    out.write("@" + readId + "\n");
    out.write(sequence + "\n");
    out.write("+\n");
    out.write(StringUtils.repeat("I", sequence.length()) + "\n");
  }

  /**
   * Create the fastq files containing the reads and the fasta files containing
   * the reference genome.
   *
   * @param num_mate_pairs: Number of mate pair files
   * @param num_contigs: Number of contigs per file.
   * @return
   */
  private TestData createInputs(
      File directory, int num_mate_pairs, int num_contigs) {
    TestData output = new TestData();
    Random generator = new Random();

    for (int i = 0; i < num_mate_pairs; ++i) {
        try {
          File referencePath =
              new File(directory, String.format("contigs_%d.fa", i));
          output.referenceFiles.add(referencePath.toString());

          File leftPath =
              new File(directory, String.format("reads_0.fastq", i));
          output.readFiles.add(leftPath.toString());

          File rightPath =
              new File(directory, String.format("reads_1.fastq", i));
          output.readFiles.add(rightPath.toString());

          FileWriter referenceStream = new FileWriter(referencePath, true);
          BufferedWriter referenceOut = new BufferedWriter(referenceStream);

          FileWriter leftStream = new FileWriter(leftPath, true);
          BufferedWriter leftOut = new BufferedWriter(leftStream);

          FileWriter rightStream = new FileWriter(rightPath, true);
          BufferedWriter rightOut = new BufferedWriter(rightStream);

          // TODO(jlewi): To create a better test we should make the reads
          // come from different contigs.
          for (int r = 0; r < num_contigs; r++) {
            String contigId = String.format("contig_%d_%d\n", i, r);
            String sequence =
                AlphabetUtil.randomString(
                    generator, 100, DNAAlphabetFactory.create());

            String leftId = String.format("read_left_%d_%d", i, r);
            writeFastQRecord(
                leftOut, leftId, sequence.substring(0, 30));

            String rightId = String.format("read_right_%d_%d", i, r);
            writeFastQRecord(
                rightOut, rightId, sequence.substring(70, 100));

            referenceOut.write(">" + contigId);
            referenceOut.write(sequence);
            referenceOut.write("\n");
          }

          referenceOut.close();
          referenceStream.close();
          leftOut.close();
          leftStream.close();
          rightOut.close();
          rightStream.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
    }
    output.readsGlob = FilenameUtils.concat(
        directory.getPath(), "*fastq");
    output.referenceGlob = FilenameUtils.concat(
        directory.getPath(), "*fa");

    // Create the library size file.
    try {
      output.libSizeFile = FilenameUtils.concat(
          directory.getPath(), "libsize");
      FileWriter stream = new FileWriter(output.libSizeFile, true);
      BufferedWriter out = new BufferedWriter(stream);

      out.write("reads 25 125\n");
      out.close();
      stream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return output;
  }

  public void runTests(HashMap<String, String> args) {
    File tempDir = FileHelper.createLocalTempDir();
    dirsToDelete.add(tempDir);

    final int NUM_REFERENCE_FILES = 3;
    final int NUM_CONTIGS_PER_FILE = 3;
    TestData testData = createInputs(
        tempDir, NUM_REFERENCE_FILES, NUM_CONTIGS_PER_FILE);


    BuildBambusInput stage = new BuildBambusInput();

    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.putAll(args);
    parameters.put("reads_glob",testData.readsGlob);
    parameters.put("reference_glob",testData.referenceGlob);
    parameters.put("libsize", testData.libSizeFile);
    parameters.put(
        "outputpath", FilenameUtils.concat(
            tempDir.getPath(), "output"));
    stage.setParameters(parameters);
    parameters.put("outprefix", "output");

    stage.setParameters(parameters);
    try {
      stage.runJob();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("test failed!");
    }

    // Check the output files exist.
    if (!stage.getFastaOutputFile().exists()) {
      throw new RuntimeException("test failed");
    }
    if (!stage.getLibraryOutputFile().exists()) {
      throw new RuntimeException("test failed");
    }
    if (!stage.getContigOutputFile().exists()) {
      throw new RuntimeException("test failed");
    }
  }

  @Override
  protected void finalize() {
    // Cleanup the test.
    ArrayList<String> errors = new ArrayList<String> ();
    for (File dir : dirsToDelete) {
      try {
        FileUtils.deleteDirectory(dir);
      } catch (IOException e) {
        errors.add(
            "Couldn't delete the temporary directory: " +
                dir.getAbsolutePath() + " \n. Exception was:" + e.getMessage());
      }
    }

    if (errors.size() >0 ) {
      throw new RuntimeException(
          "There was a problem cleaning up the test errors:" +
              StringUtils.join(errors, "\n"));
    }
  }

  public static void main(String[] args) {
    if(args.length !=2 ){
      throw new RuntimeException(
          "Expected two arguments bowtie_path and bowtiebuild_path");
    }
    HashMap<String, String> parameters = new HashMap<String, String>();

    for (String arg : args) {
      String[] pieces = arg.split("=", 2);
      pieces[0] = pieces[0].replaceFirst("-*", "");
      parameters.put(pieces[0], pieces[1]);
    }

    String[] required = {"bowtie_path", "bowtiebuild_path"};
    ArrayList<String> missing = new ArrayList<String> ();

    for (String arg : required) {
      if (parameters.containsKey(arg)) {
        continue;
      }
      missing.add(arg);
    }

    if (missing.size() > 0) {
      throw new RuntimeException(
          "Missing arguments:" + StringUtils.join(missing, ","));
    }
    TestBuildBambusInput test = new TestBuildBambusInput();
    test.runTests(parameters);
  }
}
