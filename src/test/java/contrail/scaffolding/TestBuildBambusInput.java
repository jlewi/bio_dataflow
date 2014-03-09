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

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;
import contrail.sequences.Sequence;
import contrail.util.AvroFileUtil;
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
public class TestBuildBambusInput  {
  private static class TestData {
    public TestData() {
      referenceFiles = new ArrayList<String>();
      libraries = new ArrayList<Library>();
      mappingFiles = new ArrayList<String>();
      reads = new HashMap<String, String>();
      matePairs = new HashMap<String, HashSet<String>>();
    }
    // Files containing the sequences we want to align to.
    public ArrayList<String> referenceFiles;

    // The read libraries.
    public ArrayList<Library> libraries;

    // The files containing the mappings resulting from bowtie.
    public ArrayList<String> mappingFiles;

    // The reads: ReadId -> Sequence.
    public HashMap<String, String> reads;

    public String referenceGlob;
    public String libFile;
    public String graphGlob;
    public String mappingsGlob;

    // Sort the reads in each pair and then join them by a comma.
    // The key is the library name. The values are a set of the mate pairs.
    public HashMap<String, HashSet<String>> matePairs;

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
   * Create the following inputs for the test:
   *   1. The fastq files containing the reads.
   *   2. The fasta files containing the reference genome.
   *   3. The Avro file containing the alignments of the reads to the reference.
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
      Library library = new Library();
      library.setName(String.format("somelib%d", i));
      library.setFiles(new ArrayList<CharSequence>());
      library.setMinSize(1);
      library.setMaxSize(100);
      output.libraries.add(library);

      output.matePairs.put(library.getName().toString(), new HashSet<String>());

      try {
        ArrayList<GraphNode> graphNodes = new ArrayList<GraphNode>();
        File referencePath =
            new File(directory, String.format("contigs_%d.fa", i));
        output.referenceFiles.add(referencePath.toString());

        File leftPath =
            new File(directory, String.format("reads-%d_0.fastq", i));
        library.getFiles().add(leftPath.toString());

        File rightPath =
            new File(directory, String.format("reads-%d_1.fastq", i));
        library.getFiles().add(rightPath.toString());

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

          output.reads.put(leftId, sequence.substring(leftStart, leftEnd));

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

          output.reads.put(rightId, sequence.substring(rightStart, rightEnd));

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

          output.matePairs.get(library.getName().toString()).add (
              leftId + "," + rightId);

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
        output.mappingFiles.add(mappingPath);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    output.referenceGlob = FilenameUtils.concat(
        directory.getPath(), "*fa");
    output.graphGlob = FilenameUtils.concat(
        directory.getPath(), "graph*avro");
    output.mappingsGlob = FilenameUtils.concat(
        directory.getPath(), "mappings*avro");
    output.hdfsPath = FilenameUtils.concat(
        directory.getPath(), "hdfs_path");
    output.libFile =FilenameUtils.concat(
        directory.getPath(), "libraries.json");
    // Create the library size file.
    AvroFileUtil.prettyPrintJsonArray(
        new Configuration(), new Path(output.libFile), output.libraries);

    return output;
  }

  private void assertExpectedFastaFile(
      TestData testData, String fastaFile, int readLength) {
    // Read all the fasta records.
    FastaFileReader fastaReader = new FastaFileReader(fastaFile);

    HashMap<String, String> actual = new HashMap<String, String>();

    while(fastaReader.hasNext()) {
      FastaRecord read = fastaReader.next();
      actual.put(
          read.getId().toString(),
          read.getRead().subSequence(0, readLength).toString());
    }

    HashMap<String, String> expected = new HashMap<String, String>();
    for (String id: testData.reads.keySet()) {
      expected.put(id, testData.reads.get(id).substring(0,  readLength));
    }
    fastaReader.close();

    assertEquals(actual, expected);
  }

  private static class LibraryFileContents {
    public LibraryFileContents() {
      libraries = new ArrayList<Library>();
      matePairs = new HashMap<String, HashSet<String>>();
    }

    // The files in the library will be empty.
    public ArrayList<Library> libraries;

    // Sort the reads in each pair and then join them by a comma.
    // The key is the library name. The values are a set of the mate pairs.
    public HashMap<String, HashSet<String>> matePairs;
  }

  private static class NameComparator implements Comparator<Library> {
    @Override
    public int compare(Library o1, Library o2) {
      return o1.getName().toString().compareTo(o2.getName().toString());
    }
  }

  private LibraryFileContents readLibraryFile(String libraryFile) throws IOException {
      LibraryFileContents contents = new LibraryFileContents();
      BufferedReader libReader = new BufferedReader(new FileReader(
          libraryFile));

      while (true) {
        String line = libReader.readLine();
        if (line == null) {
          break;
        }
        if (line.startsWith("library")) {
          String[] pieces = line.split(" ");
          Library lib = new Library();
          lib.setName(pieces[1]);
          lib.setMinSize(Integer.parseInt(pieces[2]));
          lib.setMaxSize(Integer.parseInt(pieces[3]));
          contents.libraries.add(lib);
          contents.matePairs.put(
              lib.getName().toString(), new HashSet<String>());
        } else {
          String[] pieces = line.split(" ");
          String libName = pieces[2];
          Arrays.sort(pieces, 0, 2);
          contents.matePairs.get(libName).add(pieces[0] + "," + pieces[1]);
        }
      }

      libReader.close();
      return contents;
  }

  private void assertExpectedLibraryFile(
      TestData testData, String libraryFile) throws IOException {
    LibraryFileContents libraryContents = readLibraryFile(libraryFile);

    assertEquals(testData.matePairs, libraryContents.matePairs);

    Collections.sort(testData.libraries, new NameComparator());
    Collections.sort(libraryContents.libraries, new NameComparator());

    assertEquals(testData.libraries.size(), libraryContents.libraries.size());

    for (int i = 0; i < testData.libraries.size(); ++i) {
      Library expected = testData.libraries.get(i);
      Library actual = libraryContents.libraries.get(i);
      assertEquals(expected.getName().toString(), actual.getName().toString());
      assertEquals(expected.getMinSize(), actual.getMinSize());
      assertEquals(expected.getMaxSize(), actual.getMaxSize());
    }
  }

  @Test
  public void testRun() throws IOException {
    Integer readLength = 10;
    File tempDir = FileHelper.createLocalTempDir();

    final int NUM_REFERENCE_FILES = 3;
    final int NUM_CONTIGS_PER_FILE = 3;
    TestData testData = createInputs(
        tempDir, NUM_REFERENCE_FILES, NUM_CONTIGS_PER_FILE);


    BuildBambusInput stage = new BuildBambusInput();
    stage.setConf(new Configuration());
    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("reference_glob",testData.referenceGlob);
    parameters.put("bowtie_alignments", testData.mappingsGlob);
    parameters.put("library_file", testData.libFile);
    parameters.put("graph_glob", testData.graphGlob);
    parameters.put("hdfs_path", testData.hdfsPath);
    parameters.put("read_length", readLength);
    parameters.put(
        "outputpath", FilenameUtils.concat(
            tempDir.getPath(), "output"));
    parameters.put("outprefix", "output");
    stage.setParameters(parameters);

    if (!stage.execute()) {
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

    assertExpectedFastaFile(
        testData, stage.getFastaOutputFile().getAbsolutePath(), readLength);

    assertExpectedLibraryFile(
        testData, stage.getLibraryOutputFile().getAbsolutePath());
  }
}
