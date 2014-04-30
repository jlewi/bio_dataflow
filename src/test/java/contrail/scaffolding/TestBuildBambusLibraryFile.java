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
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/** UnitTest for BuildBambusLibraryFile stage. */
public class TestBuildBambusLibraryFile  {
  private static class TestData {
    public TestData() {
      libraries = new ArrayList<Library>();
      matePairs = new HashMap<String, HashSet<String>>();
    }

    // The read libraries.
    public ArrayList<Library> libraries;

    public String libFile;

    // Sort the reads in each pair and then join them by a comma.
    // The key is the library name. The values are a set of the mate pairs.
    // These are the expected entries in the library file.
    public HashMap<String, HashSet<String>> matePairs;
  }

  /**
   * Create the following inputs for the test:
   *   1. The fastq files containing the reads.
   *   2. A library file describing the libraries.
   *
   * @return
   */
  private TestData createInputs(String outputDir) {
    TestData output = new TestData();
    Random generator = new Random();

    for (int libCount = 0; libCount < 2; ++libCount) {
      Library library = new Library();
      library.setName(String.format("somelib%d", libCount));
      library.setFiles(new ArrayList<CharSequence>());
      library.setMinSize(generator.nextInt(100));
      library.setMaxSize(generator.nextInt(100));
      output.libraries.add(library);

      output.matePairs.put(library.getName().toString(), new HashSet<String>());
      for (int fileIndex = 0; fileIndex < 2; ++fileIndex) {
        ArrayList<MatePair> mates = new ArrayList<MatePair>();
        for (int mateIndex = 0; mateIndex < 2; ++mateIndex) {
          String idFormat = "somelib_%d_file_%d_mate_%d/%d";
          MatePair matePair = new MatePair();
          FastQRecord left = new FastQRecord();
          left.setId(String.format(
              idFormat, libCount, fileIndex, mateIndex, 1));
          left.setQvalue("");
          left.setRead("");
          matePair.setLeft(left);

          FastQRecord right = new FastQRecord();
          right.setId(String.format(
              idFormat, libCount, fileIndex, mateIndex, 2));
          right.setQvalue("");
          right.setRead("");
          matePair.setRight(right);

          mates.add(matePair);

          output.matePairs.get(library.getName().toString()).add(
              left.getId().toString() + "," + right.getId().toString());
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
        new Configuration(), new Path(output.libFile), output.libraries);

    return output;
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

  private LibraryFileContents readLibraryFile(
      String libraryFile) throws IOException {
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
    String tempDir = FileHelper.createLocalTempDir().toString();

    TestData testData = createInputs(tempDir);

    BuildBambusLibraryFile stage = new BuildBambusLibraryFile();
    stage.setConf(new Configuration());
    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("library_file", testData.libFile);

    String matesFile = FilenameUtils.concat(tempDir, "mates.txt");
    parameters.put("outputpath", matesFile);
    stage.setParameters(parameters);

    if (!stage.execute()) {
      throw new RuntimeException("test failed!");
    }

    assertExpectedLibraryFile(testData, matesFile);
  }
}
