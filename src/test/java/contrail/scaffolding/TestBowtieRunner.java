package contrail.scaffolding;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

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
    ArrayList<String> fastaFiles = createFastaFiles(tempDir, 3);
    FileUtils.deleteDirectory(tempDir);

    String outBase = new File(tempDir, "index").getAbsolutePath();
    runner.bowtieBuildIndex(fastaFiles, outBase);
  }

  public static void main(String[] args) {
      if(args.length !=2 ){
        throw new RuntimeException(
            "Expected two arguments bowtie_path and bowtiebuild_path");
      }
      HashMap<String, String> parameters = new HashMap<String, String>();

      for (String arg : args) {
        String[] pieces = arg.split("=", 1);
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
