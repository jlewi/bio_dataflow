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
package contrail.stages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;

import contrail.io.FastQInputFormat;
import contrail.util.ContrailLogger;
import contrail.util.FileHelper;

/**
 * Reverse the reads in a bunch of files.
 *
 * Each file is processed individually in a non-parallel way so that we can
 * assign the output the same name as the original input. We do this
 * because scaffolding relies on the filename to match reads to libraries.
 */
public class ReverseReadsForScaffolding extends NonMRStage {
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      ReverseReadsForScaffolding.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String,
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def :
         ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  protected void rename(Path oldPath, Path newPath) {
    FileSystem fs = null;
    try{
      fs = oldPath.getFileSystem(getConf());
    } catch (IOException e) {
      sLogger.fatal("Can't get filesystem: " + e.getMessage());
    }
    try {
      fs.rename(oldPath, newPath);
    } catch (IOException e) {
      sLogger.fatal("Problem moving the file: " + e.getMessage());
    }
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) this.stage_options.get("inputpath");
    ArrayList<Path> readFiles = FileHelper.matchListOfGlobsWithDefault(
        getConf(), inputPath, "*");
    if (readFiles.isEmpty()) {
      sLogger.fatal("No files found in:" + inputPath);
    }

    String outputPath = (String) this.stage_options.get("outputpath");

    String tempDir = FilenameUtils.concat(outputPath, "temp");
    for (int i = 0; i < readFiles.size(); ++i) {
      ReverseReads stage = new ReverseReads();
      JobConf conf = new JobConf(getConf());

      // This is only a hint. We may need to modify the input splits
      // in order to generate a single input split.
      conf.setNumMapTasks(1);
      stage.setConf(conf);

      Path readFile = readFiles.get(i);
      long fileLength = 0;
      try {
        FileSystem fs = readFile.getFileSystem(getConf());
        FileStatus[] status = fs.listStatus(readFile);
        fileLength = status[0].getLen();
      } catch(IOException e) {
        sLogger.fatal("Could not determine the length of the file.", e);
      }

      sLogger.info("Set split size to:" + fileLength);
      // Set the input size to twice the file length so that there will be
      // a single file split.
      conf.setLong(FastQInputFormat.SPLIT_SIZE_NAME, fileLength * 2);
      stage.setParameter("inputpath", readFile.toString());
      stage.setParameter("outputpath", tempDir);

      if (!stage.execute()) {
        sLogger.fatal(
            "Could not reverse the reads in: " + readFile.toString());
      }

      // We need to rename the file.
      String name = FilenameUtils.getName(readFile.toString());

      ArrayList<Path> outFiles = FileHelper.matchGlobWithDefault(
          getConf(), tempDir, "part-*");

      if (outFiles.size() != 1) {
        sLogger.fatal(String.format(
            "There should be a single output file but there were %d output " +
            "files.", outFiles.size()));
      }
      Path newPath = new Path(FilenameUtils.concat(outputPath, name));
      rename(outFiles.get(0), newPath);
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new ReverseReadsForScaffolding(), args);
    System.exit(res);
  }
}
