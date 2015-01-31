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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.avro.specific.SpecificData;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;
import contrail.sequences.MatePair;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/** Unittest for BuildBambusFastaFile */
public class TestBuildBambusFastaFile {
  private static class TestData {
    public TestData() {
      expectedReads = new HashMap<String, FastaRecord>();
    }

    // The expected shortened reads.
    HashMap<String, FastaRecord> expectedReads;

    // The location of the library file.
    public String libFile;
  }

  /**
   * Create the following inputs for the test:
   *   1. The fastq files containing the reads.
   *   2. A library file describing the libraries.
   *
   * @return
   */
  private TestData createInputs(String outputDir, int readLength) {
    TestData output = new TestData();
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
            output.expectedReads.put(fasta.getId().toString(), fasta);

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

    return output;
  }

  private void assertExpectedFastaFile(
      TestData testData, String fastaFile, int readLength) {
    // Read all the fasta records.
    FastaFileReader fastaReader = new FastaFileReader(fastaFile);

    HashMap<String, FastaRecord> actual = new HashMap<String, FastaRecord>();

    while(fastaReader.hasNext()) {
      FastaRecord read = fastaReader.next();
      read = SpecificData.get().deepCopy(read.getSchema(), read);
      actual.put(read.getId().toString(), read);
    }

    fastaReader.close();
    assertEquals(actual, testData.expectedReads);
  }


  @Test
  public void testRun() throws IOException {
    String tempDir = FileHelper.createLocalTempDir().toString();

    int readLength = 25;
    TestData testData = createInputs(tempDir, readLength);

    BuildBambusFastaFile stage = new BuildBambusFastaFile();
    stage.setConf(new Configuration());
    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("library_file", testData.libFile);
    parameters.put("read_length", readLength);
    String fastaFile = FilenameUtils.concat(tempDir, "reads.fasta");
    parameters.put("outputpath", fastaFile);
    stage.setParameters(parameters);

    if (!stage.execute()) {
      throw new RuntimeException("test failed!");
    }

    assertExpectedFastaFile(testData, fastaFile, readLength);
  }
}
