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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/**
 * A binary useful for testing the code for aligning the reads using bowtie.
 *
 * This class needs to be run manually because you need to specify the
 * path to bowtie.
 *
 * The test only checks whether the code runs without errors; it doesn't
 * verify that the output is correct.
 *
 * TODO(jeremy@lewi.us): We could try to make this an automatic unittest
 * by searching the path for bowtie and bowtie-build so that the user
 * doesn't have to specify the manually.
 */
public class TestAlignReadsWithBowtie extends AlignReadsWithBowtie {

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

    // Temporary directory to be used by the mapreduces launched by
    // BuildBambusInput.
    public String hdfsPath;

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
          ArrayList<GraphNode> graphNodes = new ArrayList<GraphNode>();
          File referencePath =
              new File(directory, String.format("contigs_%d.fa", i));
          output.referenceFiles.add(referencePath.toString());

          File leftPath =
              new File(directory, String.format("reads-%d_0.fastq", i));
          output.readFiles.add(leftPath.toString());

          File rightPath =
              new File(directory, String.format("reads-%d_1.fastq", i));
          output.readFiles.add(rightPath.toString());

          FileWriter referenceStream = new FileWriter(referencePath, true);
          BufferedWriter referenceOut = new BufferedWriter(referenceStream);

          FileWriter leftStream = new FileWriter(leftPath, true);
          BufferedWriter leftOut = new BufferedWriter(leftStream);

          FileWriter rightStream = new FileWriter(rightPath, true);
          BufferedWriter rightOut = new BufferedWriter(rightStream);

          ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();

          // TODO(jlewi): To create a better test we should make the reads
          // come from different contigs.
          String readFormat = "read_lib_%d_contig_%d_mate_%d";

          for (int r = 0; r < num_contigs; r++) {
            String contigId = String.format("contig_%d_%d", i, r);
            String sequence =
                AlphabetUtil.randomString(
                    generator, 100, DNAAlphabetFactory.create());

            int leftMateSuffix = 1;
            String leftId = String.format(readFormat, i, r, leftMateSuffix);
            int leftStart = 0;
            int leftEnd = 30;
            writeFastQRecord(
                leftOut, leftId, sequence.substring(leftStart, leftEnd));

            BowtieMapping leftMapping = new BowtieMapping();
            leftMapping.setContigId(contigId);
            leftMapping.setReadId(leftId);
            leftMapping.setContigStart(leftStart);
            leftMapping.setContigEnd(leftEnd);
            leftMapping.setNumMismatches(0);
            leftMapping.setReadClearStart(leftStart);
            leftMapping.setReadClearEnd(leftEnd);

            mappings.add(leftMapping);

            int rightMateSuffix = 2;
            String rightId = String.format(readFormat, i, r, rightMateSuffix);
            int rightStart = 70;
            int rightEnd = 100;
            writeFastQRecord(
                rightOut, rightId, sequence.substring(rightStart, rightEnd));

            referenceOut.write(">" + contigId + "\n");
            referenceOut.write(sequence);
            referenceOut.write("\n");

            BowtieMapping rightMapping = new BowtieMapping();
            rightMapping.setContigId(contigId);
            rightMapping.setReadId(rightId);
            rightMapping.setContigStart(rightStart);
            rightMapping.setContigEnd(rightEnd);
            rightMapping.setNumMismatches(0);
            rightMapping.setReadClearStart(rightStart);
            rightMapping.setReadClearEnd(rightEnd);

            mappings.add(rightMapping);

            // Create a graph node to represent the contig.
            GraphNode node = new GraphNode();
            node.setNodeId(contigId);
            node.setSequence(new Sequence(
                sequence, DNAAlphabetFactory.create()));
            graphNodes.add(node);
          }

          referenceOut.close();
          referenceStream.close();
          leftOut.close();
          leftStream.close();
          rightOut.close();
          rightStream.close();

          String graphPath = FilenameUtils.concat(
              directory.getPath(), String.format("graph_%d.avro", i));

          GraphUtil.writeGraphToFile(new File(graphPath), graphNodes);

          String mappingPath = FilenameUtils.concat(
              directory.getPath(), String.format("mappings_%d.avro", i));
          AvroFileUtil.writeRecords(
              new Configuration(), new Path(mappingPath), mappings);
          //output.mappingFiles.add(mappingPath);

        } catch (Exception e) {
          e.printStackTrace();
        }
    }
    output.readsGlob = FilenameUtils.concat(
        directory.getPath(), "*fastq");
    output.referenceGlob = FilenameUtils.concat(
        directory.getPath(), "*fa");
    output.hdfsPath = FilenameUtils.concat(
        directory.getPath(), "hdfs_path");
    // Create the library size file.
    try {
      output.libSizeFile = FilenameUtils.concat(
          directory.getPath(), "libsize");
      FileWriter stream = new FileWriter(output.libSizeFile, true);
      BufferedWriter out = new BufferedWriter(stream);

      for (int i = 0; i < num_mate_pairs; ++i) {
        out.write(String.format("reads-%d 25 125\n", i));
      }
      out.close();
      stream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return output;
  }

  public void runTests(HashMap<String, String> args) {
    File tempDir = FileHelper.createLocalTempDir();

    final int NUM_REFERENCE_FILES = 3;
    final int NUM_CONTIGS_PER_FILE = 3;
    TestData testData = createInputs(
        tempDir, NUM_REFERENCE_FILES, NUM_CONTIGS_PER_FILE);


    AlignReadsWithBowtie stage = new AlignReadsWithBowtie();

    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.putAll(args);
    parameters.put("reads_glob",testData.readsGlob);
    parameters.put("reference_glob",testData.referenceGlob);
    parameters.put("hdfs_path", testData.hdfsPath);
    parameters.put("local_path", FilenameUtils.concat(
        tempDir.getAbsolutePath(), "local"));

    stage.setParameters(parameters);

    if (!stage.execute()) {
      throw new RuntimeException("test failed!");
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
    TestAlignReadsWithBowtie test = new TestAlignReadsWithBowtie();
    test.runTests(parameters);
  }
}
