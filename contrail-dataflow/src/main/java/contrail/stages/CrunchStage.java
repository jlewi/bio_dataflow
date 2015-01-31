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

import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public abstract class CrunchStage extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(CrunchStage.class);

  /**
   * Helper function to delete existing paths when starting a stage.
   * @param outPath
   */
  protected void deleteExistingPath(Path outPath) {
    try {
      FileSystem outFs = outPath.getFileSystem(getConf());
      if (outFs.exists(outPath)) {
        // TODO(jlewi): We should only delete an existing directory
        // if explicitly told to do so.
        sLogger.info("Deleting output path: " + outPath.toString() + " "
            + "because it already exists.");
        outFs.delete(outPath, true);
      }
    } catch (IOException e) {
      sLogger.fatal(
          "There was a problem checking if the directory:" +
              outPath.toString() + " exists and deleting it if it does.", e);
      System.exit(-1);
    }
  }

  /**
   * Print the counters.
   *
   * @param result
   */
  protected void printCounters(PipelineResult result) {
    // TODO(jlewi): How can we sort the stage results by their approximate
    // execution order?
    for (StageResult stageResult : result.getStageResults()) {
      sLogger.info("Stage: " + stageResult.getStageName());
      sLogger.info("Counters:");

      for (String group : stageResult.getCounterNames().keySet()) {
        sLogger.info(group + ":");
        ArrayList<String> counters = new ArrayList<String>();
        counters.addAll(stageResult.getCounterNames().get(group));
        Collections.sort(counters);
        for (String counter : counters) {
          String displayName =
              stageResult.getCounterDisplayName(group, counter);
          long value = stageResult.getCounterValue(group,  counter);
          sLogger.info(String.format("     %s: %d", displayName, value));
        }
      }
    }
  }
}

