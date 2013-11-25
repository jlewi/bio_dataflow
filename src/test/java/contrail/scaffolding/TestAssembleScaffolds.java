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
import java.util.Collection;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.sequences.FastaRecord;
import contrail.sequences.Sequence;
import contrail.util.FileHelper;

/**
 * A binary useful for testing the code for building the bambus input.
 *
 * This class needs to be run manually because you need to specify the
 * paths for the various binaries (e.g bowtie, bowtie-build, goBambus2, etc...)
 *
 * TODO(jeremy@lewi.us): We could automate this test by using dependency
 * injection to inject a fake executor for shell commands. We could then
 * verify that the expected command is called. We could then copy some
 * canned data to the appropriate directories so that subsequent stages
 * can proceed as expected.
 *
 * TODO(jeremy@lewi.us): We could try to make this an automatic unittest
 * by searching the path for bowtie and bowtie-build and bambus so that the user
 * doesn't have to specify them manually.
 */
public class TestAssembleScaffolds {
  private static final Logger sLogger = Logger.getLogger(
      TestAssembleScaffolds.class);
  private final ArrayList<File> dirsToDelete;

  private String testDir;
  public TestAssembleScaffolds() {
    dirsToDelete = new ArrayList<File>();
  }

  private static class TestData {
    public TestData() {
      referenceFiles = new ArrayList<String>();
      readFiles = new ArrayList<String>();
      degenerateContigs = new HashMap<String, FastaRecord>();
    }
    // Files containing the sequences we want to align to.
    public ArrayList<String> referenceFiles;

    // The short reads to align to the reference.
    public ArrayList<String> readFiles;

    public String readsGlob;
    public String referenceGlob;
    public String libSizeFile;
    public String graphGlob;
    public String hdfsPath;

    // The true sequence for the scaffold.
    public String expectedScaffold;

    // A degenerate contig; i.e. a contig not in any scaffolds.
    public HashMap<String, FastaRecord> degenerateContigs;
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

  private void writeFastaFile(File path, Collection<FastaRecord> contigs) {
    // Write the contigs to a fasta file.
    try {
      PrintStream stream = new PrintStream(path);
      for (FastaRecord record : contigs) {
        FastUtil.writeFastARecord(stream, record);
      }
      stream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void writeLibraryFile(String libraryFile, int minSize, int maxSize) {
    // Create the library size file.
    try {
      FileWriter stream = new FileWriter(libraryFile, false);
      BufferedWriter out = new BufferedWriter(stream);

      out.write(String.format("reads %d %d\n", minSize, maxSize));
      out.write(String.format("degeneratereads %d %d\n", minSize, maxSize));
      out.close();
      stream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void writeGraphNodes(
      String graphFile, ArrayList<FastaRecord> contigs) {
    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();

    for (FastaRecord record : contigs) {
      GraphNode node = new GraphNode();
      node.setNodeId(record.getId().toString());
      node.setSequence(
          new Sequence(
              record.getRead().toString(), DNAAlphabetFactory.create()));
      nodes.add(node);
    }
    GraphUtil.writeGraphToFile(new File(graphFile), nodes);
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

    if (!directory.exists()) {
      directory.mkdirs();
    }
    // Size for the library.
    final int MIN_SIZE = 400;
    final int MAX_SIZE = 500;

    // Size to use for the inserts
    final int INSERT_SIZE = (MIN_SIZE + MAX_SIZE) / 2;

    // Create the reference genome.
    test.expectedScaffold = AlphabetUtil.randomString
        (generator, 400, DNAAlphabetFactory.create());

    // Split the genome into contigs.
    int contigLength = 200;
    ArrayList<FastaRecord> contigs = new ArrayList<FastaRecord>();
    int position = 0;
    while (position < test.expectedScaffold.length()) {
      FastaRecord record = new FastaRecord();
      record.setId(String.format("contig-%d", position));
      record.setRead(test.expectedScaffold.substring(
          position, position + contigLength));
      contigs.add(record);
      position += contigLength;
    }

    // Write the contigs to a fasta file.
    File referencePath = new File(directory, "contigs.fa");
    test.referenceFiles.add(referencePath.toString());
    writeFastaFile(referencePath, contigs);

    // Write the contigs to an avro file containing the graph nodes.
    test.graphGlob = FilenameUtils.concat(directory.getPath(), "graph*avro");
    writeGraphNodes(
        FilenameUtils.concat(directory.getPath(), "graph.avro"), contigs);


    // Split the contigs into mate pair reads.
    ArrayList<FastQRecord> leftReads = new ArrayList<FastQRecord>();
    ArrayList<FastQRecord> rightReads = new ArrayList<FastQRecord>();
    int readLength = 30;
    for (int i=0; i < contigs.size() - 1; ++i) {
      // Generate multiple links.
      String leftContig = contigs.get(i).getRead().toString();
      String rightContig = contigs.get(i + 1).getRead().toString();
      for (int offset = 0; offset < 5; ++offset) {
        FastQRecord left = new FastQRecord();
        left.setId(String.format("read_%d_%d_0", i, offset));
        int start = contigLength - readLength - offset;
        left.setRead(leftContig.substring(start, start + readLength));
        left.setQvalue(StringUtils.repeat("!", readLength));

        FastQRecord right = new FastQRecord();
        right.setId(String.format("read_%d_%d_1", i, offset));
        right.setRead(rightContig.substring(offset, offset + readLength));
        right.setQvalue(StringUtils.repeat("!", readLength));

        leftReads.add(left);
        rightReads.add(right);
      }
    }

    // Write the reads to FastQ files.
    File leftPath = new File(directory, "reads_0.fastq");
    test.readFiles.add(leftPath.toString());

    File rightPath = new File(directory, "reads_1.fastq");
    test.readFiles.add(rightPath.toString());

    writeFastQFile(leftPath, leftReads);
    writeFastQFile(rightPath, rightReads);

    // Create a degenerate contig; i.e one with no mate pairs so it won't
    // be in any scaffold.
    ArrayList<FastaRecord> degenerateContigs = new ArrayList<FastaRecord>();
    int numDegenerate = 1;
    for (int i=0; i < numDegenerate; ++i) {
      FastaRecord record = new FastaRecord();
      record.setId(String.format("degenerate-contig-%d", i));
      record.setRead(AlphabetUtil.randomString(
          generator, contigLength, DNAAlphabetFactory.create()));
      test.degenerateContigs.put(record.getId().toString(), record);
      degenerateContigs.add(record);
    }
    File degeneratePath = new File(directory, "degenerate-contigs.fa");
    test.referenceFiles.add(degeneratePath.toString());
    writeFastaFile(degeneratePath, test.degenerateContigs.values());

    writeGraphNodes(
        FilenameUtils.concat(directory.getPath(), "graph-degenerate.avro"),
        degenerateContigs);

    ArrayList<FastQRecord> leftDegenerateReads = new ArrayList<FastQRecord>();
    ArrayList<FastQRecord> rightDegenerateReads = new ArrayList<FastQRecord>();

    int degenerateIndex = 0;
    for (FastaRecord contig : test.degenerateContigs.values()) {
      // Generate multiple links.
      String sequence = contig.getRead().toString();
      for (int offset = 0; offset < 5; ++offset) {
        FastQRecord left = new FastQRecord();
        left.setId(String.format(
            "degenerate_read_%d_%d_0", degenerateIndex, offset));
        int start = offset;
        left.setRead(sequence.substring(start, start + readLength));
        left.setQvalue(StringUtils.repeat("!", readLength));

        leftDegenerateReads.add(left);

        FastQRecord right = new FastQRecord();
        right.setId(String.format(
            "degenerate_read_%d_%d_1", degenerateIndex, offset));
        start = contigLength - readLength - offset;
        right.setRead(sequence.substring(start, start + readLength));
        right.setQvalue(StringUtils.repeat("!", readLength));
        rightDegenerateReads.add(right);
        ++degenerateIndex;
      }
    }

    // Create some reads aligned to the degenerate contig.
    // TODO(jeremy@lewi.us) Without the reads toAmos wouldn't load the contig
    // into the bank.
    File degenerateLeftFastQPath = new File(
        directory, "degenerate_reads_0.fastq");
    test.readFiles.add(degenerateLeftFastQPath.toString());
    writeFastQFile(degenerateLeftFastQPath, leftDegenerateReads);

    File degenerateRightFastQPath = new File(
        directory, "degenerate_reads_1.fastq");
    test.readFiles.add(degenerateRightFastQPath.toString());
    writeFastQFile(degenerateRightFastQPath, rightDegenerateReads);


    test.libSizeFile = FilenameUtils.concat(directory.getPath(), "libsize");
    writeLibraryFile(test.libSizeFile, MIN_SIZE, MAX_SIZE);

    test.readsGlob = FilenameUtils.concat(
        directory.getPath(), "*fastq");
    test.referenceGlob = FilenameUtils.concat(
        directory.getPath(), "*fa");
    // Working directory for the MR jobs run as part of scaffolding.
    test.hdfsPath = FilenameUtils.concat(
        directory.getPath(), "hdfs_path");
    return test;
  }

  public void runTests(HashMap<String, String> args) {
    final int NUM_REFERENCE_FILES = 3;
    final int NUM_CONTIGS_PER_FILE = 3;

    HashMap<String, Object> parameters = new HashMap<String, Object>();
    // Allow the outputpath to be overwritten on the command line.
    if (args.containsKey("testdir")) {
      testDir = args.get("testdir");
      args.remove("testdir");
    } else {
      testDir = FileHelper.createLocalTempDir().getPath();
    }

    File testDirFile = new File(testDir);
    if (testDirFile.exists()) {
      try {
        FileUtils.deleteDirectory(testDirFile);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage());
      }
    }

    parameters.put(
        "outputpath", FilenameUtils.concat(testDir, "output"));
    dirsToDelete.add(new File(testDir));

    TestData testData = createTest(
        new File(testDir, "input"), NUM_REFERENCE_FILES, NUM_CONTIGS_PER_FILE);

    parameters.putAll(args);
    parameters.put("reads_glob",testData.readsGlob);
    parameters.put("reference_glob",testData.referenceGlob);
    parameters.put("libsize", testData.libSizeFile);
    parameters.put("graph_glob", testData.graphGlob);
    parameters.put("hdfs_path", testData.hdfsPath);
    parameters.put("outprefix", "output");

    AssembleScaffolds stage = new AssembleScaffolds();
    stage.setParameters(parameters);

    if (!stage.execute()) {
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
    if(args.length < 3 ){
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
