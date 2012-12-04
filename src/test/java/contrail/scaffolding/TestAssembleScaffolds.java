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
import org.apache.commons.lang3.StringUtils;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.util.FileHelper;

/**
 * A binary useful for testing the code for building the bambus input.
 *
 * This class needs to be run manually because you need to specify the
 * paths for the various binaries (e.g bowtie, bowtie-build, goBambus2, etc...)
 *
 * TODO(jeremy@lewi.us): We could try to make this an automatic unittest
 * by searching the path for bowtie and bowtie-build and bambus so that the user
 * doesn't have to specify the manually.
 */
public class TestAssembleScaffolds {
  private final ArrayList<File> dirsToDelete;

  public TestAssembleScaffolds() {
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

    // The true sequence for the scaffold.
    public String expectedScaffold;
  }

  private void writeFastQFile(File path, ArrayList<FastQRecord> records) {
    try {
    FileWriter stream = new FileWriter(path, true);
    BufferedWriter out = new BufferedWriter(stream);

    for (FastQRecord record : records) {
      out.write("@" + record.getId() + "\n");
      out.write(record.getRead() + "\n");
      out.write("+\n");
      out.write(record.getQvalue() + "\n");
    }
    out.close();
    stream.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Failed to write the fastq file.");
    }
  }

  /**
   * Create the test data.
   *
   * We start by creating a long DNA string to act as the reference genome.
   * We then divide this genome into non-overlapping, sequential contigs.
   * These contigs are then written out to the fasta files used as one input
   * to the scaffolding scage. We then extract from the contigs mate pairs,
   * choosing one read for each pair from sequential contigs.
   *
   * @param num_mate_pairs: Number of mate pair files
   * @param num_contigs: Number of contigs per file.
   * @return
   */
  private TestData createTest(
      File directory, int num_mate_pairs, int num_contigs) {
    TestData test = new TestData();
    Random generator = new Random();

    // Create the reference genome.
    test.expectedScaffold = AlphabetUtil.randomString
        (generator, 1000, DNAAlphabetFactory.create());

    // Split the genome into contigs.
    int contigLength = 100;
    ArrayList<String> contigs = new ArrayList<String>();
    int position = 0;
    while (position < test.expectedScaffold.length()) {
      contigs.add(test.expectedScaffold.substring(
          position, position + contigLength));
      position += contigLength;
    }

    // Split the contigs into mate pair reads.
    ArrayList<FastQRecord> leftReads = new ArrayList<FastQRecord>();
    ArrayList<FastQRecord> rightReads = new ArrayList<FastQRecord>();
    int readLength = 30;
    for (int i=0; i < contigs.size() - 1; ++i) {
      int offset = 25;

      FastQRecord left = new FastQRecord();
      left.setId(String.format("read_%d_0", i));
      left.setRead(contigs.get(i).substring(offset, offset + readLength));
      left.setQvalue(StringUtils.repeat("!", readLength));

      FastQRecord right = new FastQRecord();
      right.setId(String.format("read_%d_1", i));
      right.setRead(contigs.get(i + 1).substring(offset, offset + readLength));
      right.setQvalue(StringUtils.repeat("!", readLength));

      leftReads.add(left);
      rightReads.add(right);
    }

    // Write the contigs to a fasta file.
    try {
      File referencePath = new File(directory, "contigs.fa");
      test.referenceFiles.add(referencePath.toString());

      FileWriter referenceStream = new FileWriter(referencePath, true);
      BufferedWriter referenceOut = new BufferedWriter(referenceStream);

      for (int i = 0; i < contigs.size(); ++i) {
        String contigId = String.format("contig_%d\n", i);
        referenceOut.write(">" + contigId);
        referenceOut.write(contigs.get(i));
        referenceOut.write("\n");
      }
      referenceOut.close();
      referenceStream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Write the reads to FastQ files.
    File leftPath = new File(directory, "reads_0.fastq");
    test.readFiles.add(leftPath.toString());

    File rightPath = new File(directory, "reads_1.fastq");
    test.readFiles.add(rightPath.toString());

    writeFastQFile(leftPath, leftReads);
    writeFastQFile(rightPath, rightReads);

    // Create the library size file.
    try {
      test.libSizeFile = FilenameUtils.concat(
          directory.getPath(), "libsize");
      FileWriter stream = new FileWriter(test.libSizeFile, false);
      BufferedWriter out = new BufferedWriter(stream);

      out.write("reads 25 150\n");
      out.close();
      stream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

    test.readsGlob = FilenameUtils.concat(
        directory.getPath(), "*fastq");
    test.referenceGlob = FilenameUtils.concat(
        directory.getPath(), "*fa");
    return test;
  }

  public void runTests(HashMap<String, String> args) {
    File tempDir = FileHelper.createLocalTempDir();
    dirsToDelete.add(tempDir);

    final int NUM_REFERENCE_FILES = 3;
    final int NUM_CONTIGS_PER_FILE = 3;
    TestData testData = createTest(
        tempDir, NUM_REFERENCE_FILES, NUM_CONTIGS_PER_FILE);

    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.putAll(args);
    parameters.put("reads_glob",testData.readsGlob);
    parameters.put("reference_glob",testData.referenceGlob);
    parameters.put("libsize", testData.libSizeFile);
    parameters.put(
        "outputpath", FilenameUtils.concat(
            tempDir.getPath(), "output"));
    parameters.put("outprefix", "output");

    AssembleScaffolds stage = new AssembleScaffolds();
    stage.setParameters(parameters);


    try {
      stage.runJob();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("test failed!");
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
    if(args.length !=3 ){
      throw new RuntimeException(
          "Expected 3 arguments bowtie_path, bowtiebuild_path, and amos_path");
    }
    HashMap<String, String> parameters = new HashMap<String, String>();

    for (String arg : args) {
      String[] pieces = arg.split("=", 2);
      pieces[0] = pieces[0].replaceFirst("-*", "");
      parameters.put(pieces[0], pieces[1]);
    }

    String[] required = {"bowtie_path", "bowtiebuild_path", "amos_path"};
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
    TestAssembleScaffolds test = new TestAssembleScaffolds();
    test.runTests(parameters);
  }
}
