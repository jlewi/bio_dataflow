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
import org.apache.commons.lang.StringUtils;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.util.FileHelper;

/**
 * A binary useful for testing the code for running bowtie.
 *
 * This class needs to be run manually because you need to specify the
 * path to bowtie.
 *
 * TODO(jeremy@lewi.us): We could try to make this an automatic unittest
 * by searching the path for bowtie and bowtie-build so that the user
 * doesn't have to specify the manually.
 */
public class TestBowtieRunner {
  private final ArrayList<File> dirsToDelete;

  public TestBowtieRunner() {
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
  }

  private void writeFastQRecord(
      BufferedWriter out, String readId, String sequence) throws IOException {
    out.write("@" + readId + "\n");
    out.write(sequence + "\n");
    out.write("+\n");
    out.write(StringUtils.repeat("I", sequence.length()) + "\n");
  }

  /**
   * Create the fasta files to use in the test.
   * @param num: Number of files
   * @return
   */
  private TestData createFastaFiles(File directory, int num, int num_contigs) {
    TestData output = new TestData();
    Random generator = new Random();

    for (int i = 0; i < num; ++i) {
      try {
        File referencePath =
            new File(directory, String.format("contigs_%d.fa", i));
        output.referenceFiles.add(referencePath.toString());

        File readPath =
            new File(directory, String.format("reads_%d.fastq", i));
        output.readFiles.add(readPath.toString());

        FileWriter referenceStream = new FileWriter(referencePath, true);
        BufferedWriter referenceOut = new BufferedWriter(referenceStream);

        FileWriter readStream = new FileWriter(readPath, true);
        BufferedWriter readOut = new BufferedWriter(readStream);

        for (int r = 0; r < num_contigs; r++) {
          String contigId = String.format("contig_%d_%d\n", i, r);
          String sequence =
              AlphabetUtil.randomString(
                  generator, 100, DNAAlphabetFactory.create());

          for (int offset : new int[] {0, 25, 50, 75}) {
            String readId = String.format("read_%d_%d_%d", i, r, offset);
            writeFastQRecord(
                readOut, readId, sequence.substring(offset, offset + 25));
          }

          referenceOut.write(">" + contigId);
          referenceOut.write(sequence);
          referenceOut.write("\n");
        }

        referenceOut.close();
        referenceStream.close();
        readOut.close();
        readStream.close();
      }catch (Exception e) {
        e.printStackTrace();
      }
    }
    return output;
  }

  public void runTests(HashMap<String, String> parameters) {
    BowtieRunner runner = new BowtieRunner(
        parameters.get("bowtie_path"), parameters.get("bowtiebuild_path"));

    File tempDir = FileHelper.createLocalTempDir();
    dirsToDelete.add(tempDir);

    final int NUM_REFERENCE_FILES = 3;
    final int NUM_CONTIGS_PER_FILE = 3;
    final int READ_LENGTH = 20;
    TestData testData = createFastaFiles(tempDir, NUM_REFERENCE_FILES, NUM_CONTIGS_PER_FILE);

    String indexDir = new File(tempDir, "index").getAbsolutePath();
    String indexBase = "index";
    boolean success = runner.bowtieBuildIndex(
        testData.referenceFiles, indexDir, indexBase);
    if (!success) {
      throw new RuntimeException("bowtie failed to build the index.");
    }

    String alignDir = new File(tempDir, "aligned").getAbsolutePath();
    BowtieRunner.AlignResult alignResult = runner.alignReads(
        indexDir, indexBase, testData.readFiles, alignDir, READ_LENGTH);

    // TODO(jeremy@lewi.us) we could verify the alignment succeeded because
    // each read is a substring of a contig and the name of the read tells us
    // which contig and the offset in the contig.
    if (!alignResult.success) {
      throw new RuntimeException("bowtie failed to align the reads.");
    }

    HashMap<String, ArrayList<BowtieRunner.MappingInfo>> map =
        runner.readBowtieResults(alignResult.outputs.values(), 25);

    // Check the outputs.
    for (int fileIndex = 0; fileIndex < NUM_REFERENCE_FILES; ++fileIndex) {
      for (int contigIndex = 0; contigIndex < NUM_CONTIGS_PER_FILE; ++contigIndex) {
        String contigId = String.format("contig_%d_%d", fileIndex, contigIndex);
        if (!map.containsKey(contigId)) {
          throw new RuntimeException("Missing alignment for contig:" + contigId);
        }
        // TODO(jeremy@lewi.us): We should do more extensive testing to verify the actual
        // alignments are correct.
        int numExpected = 4;
        if (map.get(contigId).size() != numExpected) {
          throw new RuntimeException(
              String.format(
                  "Expected %d reads to align to contig %s but only %d were read from the bowtie output",
                  numExpected, contigId, map.get(contigId).size()));
        }
      }
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
      TestBowtieRunner test = new TestBowtieRunner();
      test.runTests(parameters);
  }
}
