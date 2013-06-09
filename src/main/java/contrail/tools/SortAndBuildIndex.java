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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ParameterDefinition;
import contrail.stages.PipelineStage;
import contrail.stages.Stage;

/**
 * This program sorts the graph and then builds an indexed avro file for the
 * graph.
 *
 */
public class SortAndBuildIndex extends PipelineStage {
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
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    // Delete the output directory if it exists already
    Path outPath = new Path(outputPath);
    try {
      if (FileSystem.get(getConf()).exists(outPath)) {
        // TODO(jlewi): We should only delete an existing directory
        // if explicitly told to do so.
        sLogger.info("Deleting output path: " + outPath.toString() + " " +
            "because it already exists.");
        FileSystem.get(getConf()).delete(outPath, true);
      }
    } catch (IOException e) {
      sLogger.fatal("Couldn't delete:" + outputPath, e);
      System.exit(-1);
    }

    // Sort the graph.
    SortGraph sortStage = new SortGraph();
    sortStage.initializeAsChild(this);
    sortStage.setParameter("inputpath", inputPath);

    String sortedGraph = FilenameUtils.concat(outputPath, "sorted");

    sortStage.setParameter("outputpath", sortedGraph);

    executeChild(sortStage);

    CreateGraphIndex indexStage = new CreateGraphIndex();
    indexStage.initializeAsChild(this);

    String indexPath = FilenameUtils.concat(outputPath, "indexed");

    indexStage.setParameter("inputpath", sortedGraph);
    indexStage.setParameter("outputpath", indexPath);

    executeChild(indexStage);

    sLogger.info("Indexed graph written to:" + indexPath);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SortAndBuildIndex(), args);
    System.exit(res);
  }
}
