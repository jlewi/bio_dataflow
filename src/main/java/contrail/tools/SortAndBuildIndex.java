/*
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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * This program sorts the graph and then builds an indexed avro file for the
 * graph.
 *
 */
public class SortAndBuildIndex extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(SortAndBuildIndex.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    // We add all the options for the stages we depend on.
    Stage[] substages = {new SortGraph(), new CreateGraphIndex()};

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    return Collections.unmodifiableMap(definitions);
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    // Delete the output directory if it exists already
    Path outPath = new Path(outputPath);
    if (FileSystem.get(getConf()).exists(outPath)) {
      // TODO(jlewi): We should only delete an existing directory
      // if explicitly told to do so.
      sLogger.info("Deleting output path: " + outPath.toString() + " " +
          "because it already exists.");
      FileSystem.get(getConf()).delete(outPath, true);
    }

    // Sort the graph.
    SortGraph sortStage = new SortGraph();

    HashMap<String, Object> sortParameters = new HashMap<String, Object>();
    sortStage.setConf(getConf());
    sortParameters.put("inputpath", inputPath);

    String sortedGraph = FilenameUtils.concat(outputPath, "sorted");
    sortParameters.put("outputpath", sortedGraph);

    sortStage.setParameters(sortParameters);

    RunningJob job = sortStage.runJob();
    if (!job.isSuccessful()) {
      sLogger.fatal(
          "Failed to sort the graph.", new RuntimeException("Sort Failed."));
      System.exit(-1);
    }

    CreateGraphIndex indexStage = new CreateGraphIndex();
    indexStage.setConf(getConf());

    String indexPath = FilenameUtils.concat(outputPath, "indexed");
    HashMap<String, Object> indexParameters = new HashMap<String, Object>();
    indexParameters.put("inputpath", sortedGraph);
    indexParameters.put("outputpath", indexPath);

    indexStage.setParameters(indexParameters);

    indexStage.runJob();
    sLogger.info("Indexed graph written to:" + indexPath);

    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortAndBuildIndex(), args);
    System.exit(res);
  }
}
