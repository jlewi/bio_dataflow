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

package contrail.correct;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.FileHelper;

/**
 *  Cutoff calculation helps us in determining which kmers are trusted and untrusted.
 *  Trusted Kmers are kmers that have a frequency of occurrence more than a threshold
 *  (cutoff) value in the input dataset. They are used for correction, because
 *  their presence is more likely than the presence of kmer with a low frequency
 *  count (untrusted kmer). Therefore, trusted kmers are used for correction purposes
 *  by the quake engine.
 *  cov_model.py uses VGAM package within R to run some statistical analysis to
 *  calculate the cutoff value. All kmers below the cutoff are considered untrusted.
 *  This is a non MR job.
 *  For calculating the cutoff, we need a fragment of the Kmer count file in text format
 *  A small portion of the file (fragment) is enough for calculating the cutoff.
 *  A fragment of the kmer count file is copied from the HDFS onto the local system where cov_model.py is
 *  run on it to calculate the cutoff value. The value is read from the output stream of the
 *  cov_model.py process.
 */
public class CutOffCalculation extends Stage {
  private int cutoff;
  private static final Logger sLogger = Logger.getLogger(CutOffCalculation.class);

  public int getCutoff() {
    if (cutoff == 0) {
      sLogger.fatal("Cutoff wasn't calculated",
          new Exception("ERROR: Cutoff not calculated"));
      System.exit(-1);
    }
    return cutoff;
  }

  /**
   * we only need a sample of all KMers counts to compute the cutoff value
   * This method calculates the cutoff by:
   * 1. Moving only a sample of kmer count file to a temporary directory
   * 2. running cov_model.py on this temporary count file
   * 3. Reading cutoff from the standard output
   */
  public void calculateCutoff() throws Exception{
    //inputPath is the path of the file on DFS where the non avro count part is stored
    Path inputPath = new Path((String) stage_options.get("inputpath"));
    String covModelPath = (String) stage_options.get("cov_model");

    // Check if inputPath is a directory.
    if (inputPath.getFileSystem(getConf()).getFileStatus(inputPath).isDir()) {
      // Construct a glob path to match all part files.
      inputPath = new Path(FilenameUtils.concat(
          inputPath.toString(), "part-?????"));
      sLogger.info(
          "inputpath is a directory. The following glob will be used to " +
          "locate the input. Glob: " + inputPath.toString());
    }

    FileStatus[] matchedFiles =
        inputPath.getFileSystem(getConf()).globStatus(inputPath);

    if (matchedFiles.length != 1) {
      sLogger.fatal(String.format(
          "More than 1 file matched the input glob %s. Number matched:%d",
          matchedFiles.length), new IllegalArgumentException());
      System.exit(-1);
    }
    inputPath = matchedFiles[0].getPath();
    sLogger.info("Using input:" + inputPath);
    // Check if the input is already on the local filesystem and if not copy
    // it.
    // TODO(jeremy@lewi.us): What's the best way to check if its the local
    // filesystem?
    String countFile = null;
    String tempWritableFolder = null;
    if (!inputPath.getFileSystem(getConf()).getUri().getScheme().equals(
            "file")) {

      // TODO(jeremy@lewi.us): We should cleanup this temporary directory
      // after we are done.
      tempWritableFolder =
          FileHelper.createLocalTempDir().getAbsolutePath();

      FileSystem fs = FileSystem.get(getConf());
      Path localCountPath = new Path(countFile);
      sLogger.info(String.format("Copy %s to %s", inputPath, localCountPath));
      fs.copyToLocalFile(inputPath, localCountPath);
    } else {
      countFile = inputPath.toUri().getPath();
    }

    // command to run cov_model.py
    String command = covModelPath+" --int "+ countFile;
    cutoff = executeCovModel(command);

    // Clean up the temporary directory if we created one.
    if (tempWritableFolder != null) {
      File tempFile = new File(tempWritableFolder);
      if(tempFile.exists()){
        FileUtils.deleteDirectory(tempFile);
      }
    }
  }

  private int executeCovModel(String command) throws Exception {
    // TODO(jeremy@lewi.us): This is very brittle we aren't detecting whether
    // the cutoff calculation succeeded or failed. We should use the functions
    // in ShellUtil. We should redirect the output
    // Furthermore, the python script is firing off an R script so R is
    // required.
    // It might be easier if we just called the R script directly ourselves
    // rather than using cov_model.py
    StringTokenizer tokenizer;
    String line;
    int calculatedCutoff = 0;
    sLogger.info("Cutoff Calculation: "+ command);
    Process p = Runtime.getRuntime().exec(command);
    BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
    sLogger.info("Running cov_model.py");
    while ((line = stdInput.readLine()) != null) {
      tokenizer = new StringTokenizer(line);
      /* Everything displayed by the execution of cov_model.py here is stored in tokenizer
       * line by line. In the end, the token containing
       * the cutoff is taken out
       */
      if(tokenizer.hasMoreTokens() && tokenizer.nextToken().trim().equals("Cutoff:")){
        String ss = tokenizer.nextToken();
        calculatedCutoff = Integer.parseInt(ss);
        break;
      }
   }
   p.waitFor();
   return calculatedCutoff;
  }

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    ParameterDefinition QuakeHome = new ParameterDefinition(
        "cov_model", "location of cov_model.py", String.class, null);
    for (ParameterDefinition def: new ParameterDefinition[] {QuakeHome}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public RunningJob runJob(){
    // Check for missing arguments.
    String[] required_args = {"cov_model"};
    checkHasParametersOrDie(required_args);
    logParameters();
    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }
    setConf(conf);
    try{
      calculateCutoff();
      sLogger.info("Cutoff: " + getCutoff());
    }
    catch(Exception e){
      sLogger.fatal("Failed to compute cutoff.", e);
      System.exit(-1);
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CutOffCalculation(), args);
    System.exit(res);
  }
}
