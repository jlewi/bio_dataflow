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
// Author: Avijit Gupta (mailforavijit@gmail.com)
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
  private ArrayList<File> dirsToDelete;

  public TestBowtieRunner() {
    dirsToDelete = new ArrayList<File>();
  }

  /**
   * Create the fasta files to use in the test.
   * @param num: Number of files
   * @return
   */
  private ArrayList<String> createFastaFiles(File directory, int num) {
    ArrayList<String> files = new ArrayList<String>();

    Random generator = new Random();

    for (int i = 0; i < num; ++i) {
      try {
        File filePath = new File(directory, String.format("contigs_%d.fa", i));
        files.add(filePath.toString());
        FileWriter fstream = new FileWriter(filePath, true);
        BufferedWriter out = new BufferedWriter(fstream);

        for (int r = 0; r < 3; r++) {
          String sequence =
              AlphabetUtil.randomString(
                  generator, 100, DNAAlphabetFactory.create());

          out.write(String.format(">read_%d_%d\n", i, r));
          out.write(sequence);
          out.write("\n");
        }

        out.close();
        fstream.close();
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }
    return files;
  }

  public void runTests(HashMap<String, String> parameters) {
    BowtieRunner runner = new BowtieRunner(
        parameters.get("bowtie_path"), parameters.get("bowtiebuild_path"));

    File tempDir = FileHelper.createLocalTempDir();
    dirsToDelete.add(tempDir);

    ArrayList<String> fastaFiles = createFastaFiles(tempDir, 3);

    String outBase = new File(tempDir, "index").getAbsolutePath();
    boolean success = runner.bowtieBuildIndex(fastaFiles, outBase, "index");
    if (!success) {
      throw new RuntimeException("bowtie failed to build the index");
    }
  }

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
