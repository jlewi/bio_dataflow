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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastaRecord;
import contrail.sequences.MatePair;
import contrail.sequences.Sequence;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/**
 * A minimal unit test for BuildBambusInput.
 *
 * We only test that the stage executes and the output exists. We
 * rely on the unittest for the substages (TestBuildBambusFastaFile,
 * TestBuildBambusLibraryFile, and TestTigrCreator) to validate the output.
 */
public class TestBuildBambusInput  {
  private static class TestData {
    public TestData() {
      contigIds = new ArrayList<String>();
      readIds = new ArrayList<String>();
    }
    public String libFile;
    public String graphGlob;
    public String mappingsGlob;

    public ArrayList<String> contigIds;
    public ArrayList<String> readIds;
  }

  /**
   * Create the following inputs for the test:
   *   1. The fastq files containing the reads.
   *   2. A library file describing the libraries.
   *
   * @return
   */
  private void createLibraryAndReadInputs(
      TestData output, String outputDir, int readLength) {
    Random generator = new Random();

    ArrayList<Library> libraries = new ArrayList<Library>();
    for (int libCount = 0; libCount < 2; ++libCount) {
      Library library = new Library();
      library.setName(String.format("somelib%d", libCount));
      library.setFiles(new ArrayList<CharSequence>());
      library.setMinSize(generator.nextInt(100));
      library.setMaxSize(generator.nextInt(100));
      libraries.add(library);

      Alphabet alphabet = DNAAlphabetFactory.create();

      for (int fileIndex = 0; fileIndex < 2; ++fileIndex) {
        ArrayList<MatePair> mates = new ArrayList<MatePair>();
        for (int mateIndex = 0; mateIndex < 2; ++mateIndex) {
          String idFormat = "somelib_%d_file_%d_mate_%d/%d";
          MatePair matePair = new MatePair();

          for (int pairIndex : new int[]{1, 2}) {
            FastaRecord fasta = new FastaRecord();
            fasta.setId(String.format(
                idFormat, libCount, fileIndex, mateIndex, pairIndex));
            fasta.setRead(AlphabetUtil.randomString(
                generator, readLength, alphabet));

            FastQRecord read = new FastQRecord();
            read.setId(fasta.getId().toString());
            read.setQvalue("");
            read.setRead(fasta.getRead().toString() +
                AlphabetUtil.randomString(generator, 50, alphabet));

            if (pairIndex == 1) {
              matePair.setLeft(read);
            } else {
              matePair.setRight(read);
            }
            output.readIds.add(fasta.getId().toString());
          }

          mates.add(matePair);
        }

        Path outPath = new Path(FilenameUtils.concat(
            outputDir,
            String.format("lib_%d_mates_%d.avro", libCount, fileIndex)));
        library.getFiles().add(outPath.toString());
        AvroFileUtil.writeRecords(new Configuration(), outPath, mates);
      }
    }

    output.libFile = FilenameUtils.concat(outputDir, "libraries.json");

    // Create the library size file.
    AvroFileUtil.prettyPrintJsonArray(
        new Configuration(), new Path(output.libFile), libraries);
  }

  /** Create the contigs. */
  private void createContigs(TestData output, String outputDir) {
    Random generator = new Random();
    String graphDir = FilenameUtils.concat(outputDir, "contigs");
    output.graphGlob = FilenameUtils.concat(graphDir, "*.avro");
    new File(graphDir).mkdirs();

    ArrayList<GraphNode> graphNodes = new ArrayList<GraphNode>();
    for (int i = 0; i < 10; ++i) {
      // Create a graph node to represent the contig.
      GraphNode node = new GraphNode();
      node.setNodeId("contig_" + i);
      String sequence = AlphabetUtil.randomString(
          generator, 50, DNAAlphabetFactory.create());
      node.setSequence(new Sequence(
          sequence, DNAAlphabetFactory.create()));
      graphNodes.add(node);

      output.contigIds.add(node.getNodeId());
    }

    String graphPath = FilenameUtils.concat(graphDir, "contigs.avro");
    GraphUtil.writeGraphToFile(new File(graphPath), graphNodes);
  }

  /** Create the bowtie mappings. */
  private void createBowtieMappings(TestData testData, String outputDir) {
    Random generator = new Random();

    String bowtieDir = FilenameUtils.concat(outputDir, "mappings");
    new File(bowtieDir).mkdirs();
    testData.mappingsGlob = FilenameUtils.concat(bowtieDir, "*.avro");

    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();
    for (int i = 0; i < 10; ++i) {
      BowtieMapping leftMapping = new BowtieMapping();
      String contigId = testData.contigIds.get(
          generator.nextInt(testData.contigIds.size()));

      String readId = testData.readIds.get(
          generator.nextInt(testData.readIds.size()));

      leftMapping.setContigId(contigId);
      leftMapping.setReadId(readId);
      leftMapping.setContigStart(10);
      leftMapping.setContigEnd(30);
      leftMapping.setNumMismatches(0);
      leftMapping.setReadClearStart(10);
      leftMapping.setReadClearEnd(30);

      mappings.add(leftMapping);
    }
    String bowtiePath = FilenameUtils.concat(bowtieDir, "mappings.avro");
    AvroFileUtil.writeRecords(
        new Configuration(), new Path(bowtiePath), mappings);
  }

  @Test
  public void testRun() throws IOException {
    Integer readLength = 10;
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();

    TestData testData = new TestData();

    // Create the input data.
    createLibraryAndReadInputs(testData, tempDir, readLength);
    createContigs(testData, tempDir);
    createBowtieMappings(testData, tempDir);

    BuildBambusInput stage = new BuildBambusInput();
    stage.setConf(new Configuration());

    stage.setParameter("bowtie_alignments", testData.mappingsGlob);
    stage.setParameter("library_file", testData.libFile);
    stage.setParameter("graph_glob", testData.graphGlob);

    // Temporary directory to be used by the mapreduces launched by
    // BuildBambusInput.
    String hdfsPath = FilenameUtils.concat(tempDir, "temp_hdfs");
    stage.setParameter("hdfs_path", hdfsPath);
    stage.setParameter("read_length", readLength);


    stage.setParameter(
        "outputpath", FilenameUtils.concat(tempDir, "output"));
    stage.setParameter("outprefix", "bambus_input");

    if (!stage.execute()) {
      throw new RuntimeException("test failed!");
    }

    // Check the output files exist.
    for (String out : new String[] {
         stage.getFastaOutputFile(), stage.getLibraryOutputFile(),
         stage.getContigOutputFile()}) {
      File outFile = new File(out);
      if (!outFile.exists()) {
        throw new RuntimeException("test failed");
      }
    }
  }
}
