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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 
  public int getCutoff() throws Exception{
    if(cutoff!=0){
      return cutoff;
    }
	else throw new Exception("ERROR: Cutoff not calculated");
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
    String inputPath = (String) stage_options.get("inputpath");
    String covModelPath = (String) stage_options.get("cov_model");
    String tempWritableFolder = FileHelper.createLocalTempDir().getAbsolutePath();
    File countFile = new File(tempWritableFolder, "kmerCountFile");
    if (countFile.exists()){
      countFile.delete();
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path hdfsInputPath = new Path(inputPath);
    Path localCountPath = new Path(countFile.getAbsolutePath());
    fs.copyToLocalFile(hdfsInputPath, localCountPath);
    // command to run cov_model.py
    String command = covModelPath+" --int "+ countFile.getAbsolutePath();
    cutoff = executeCovModel(command);
    File tempFile = new File(tempWritableFolder);
    if(tempFile.exists()){
      FileUtils.deleteDirectory(tempFile);
    }
  }
	
  private int executeCovModel(String command) throws Exception{
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
    "cov_model", "location of cov_model.py", String.class, new String(""));
    for (ParameterDefinition def: new ParameterDefinition[] {QuakeHome}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public RunningJob runJob(){
    try{
    calculateCutoff();
    sLogger.info("Cutoff: " + getCutoff());
    }
    catch(Exception e){
      sLogger.error(e.getStackTrace());
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CutOffCalculation(), args);
    System.exit(res);
  }
}
