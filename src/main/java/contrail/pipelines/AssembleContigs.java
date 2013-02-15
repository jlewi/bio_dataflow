/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
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

package contrail.pipelines;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import contrail.stages.BuildGraphAvro;
import contrail.stages.CompressAndCorrect;
import contrail.stages.FastqPreprocessorAvroCompressed;
import contrail.stages.GraphStats;
import contrail.stages.GraphToFasta;
import contrail.stages.ParameterDefinition;
import contrail.stages.PipelineStage;
import contrail.stages.QuickMergeAvro;
import contrail.stages.StageBase;
import contrail.stages.StageInfoWriter;

/**
 * A pipeline for assembling the contigs.
 */
public class AssembleContigs extends PipelineStage {
  private static final Logger sLogger = Logger.getLogger(AssembleContigs.class);

  public AssembleContigs() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    // We add all the options for the stages we depend on.
    StageBase[] substages =
      {new FastqPreprocessorAvroCompressed(), new BuildGraphAvro(),
       new QuickMergeAvro(), new CompressAndCorrect(), new GraphStats()};

    for (StageBase stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    ParameterDefinition inFormat = new ParameterDefinition(
        "input_format",
        "String specifying the input format [avro, fastq].",
        String.class, "fastq");

    definitions.put(inFormat.getName(), inFormat);
    return Collections.unmodifiableMap(definitions);
  }

  protected void setDefaultParameters() {
    // This function is intended to be overloaded in subclasses which
    // customize the parameters for different datasets.
  }

  private void processGraph() throws Exception {
    // TODO(jlewi): Does this function really need to throw an exception?
    String outputPath = (String) stage_options.get("outputpath");

    infoWriter = new StageInfoWriter(
        getConf(), FilenameUtils.concat(outputPath,  "stage_info"));

    ArrayList<StageBase> subStages = new ArrayList<StageBase>();

    String inFormat = (String) stage_options.get("input_format");
    inFormat = inFormat.toLowerCase();

    if (inFormat.equals("fastq")) {
      subStages.add(new FastqPreprocessorAvroCompressed());
    }

    subStages.add(new BuildGraphAvro());
    subStages.add(new QuickMergeAvro());
    subStages.add(new CompressAndCorrect());
    subStages.add(new GraphToFasta());

    // The path to process for the next stage.
    String latestPath = (String) stage_options.get("inputpath");

    for (StageBase stage : subStages) {
      stage.initializeAsChild(this);

      String stageOutput =
          new Path(outputPath, stage.getClass().getName()).toString();
      stage.setParameter("inputpath", latestPath);
      stage.setParameter("outputpath", stageOutput);

      if (!executeChild(stage)) {
        sLogger.fatal(String.format(
            "Stage %s had a problem", stage.getClass().getName()),
            new RuntimeException("Stage failure."));
        System.exit(-1);
      }

      latestPath = stageOutput;
      // We compute graph stats only after the CompressAndCorrectStage
      if (CompressAndCorrect.class.isInstance(stage)){
        // TODO(jlewi): It would probably be better to continue running the
        // pipeline and not blocking on GraphStats.
        GraphStats statsStage = new GraphStats();
        statsStage.initializeAsChild(this);
        String statsOutput = new Path(
            outputPath,
            String.format("%sStats", stage.getClass().getName())).toString();

        statsStage.setParameter("inputpath", stageOutput);
        statsStage.setParameter("outputpath", statsOutput);
        if (!executeChild(statsStage)) {
          sLogger.fatal(String.format(
              "Computing stats for Stage %s had a problem",
              stage.getClass().getName()),
              new RuntimeException("Stats failure"));
          System.exit(-1);
        }
      }
    }
  }

  @Override
  public void stageMain() {
    try {
      processGraph();
    } catch (Exception e) {
      sLogger.fatal("AssembleContigs failed.", e);
      System.exit(-1);
    }
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> items = super.validateParameters();

    String inFormat = (String) stage_options.get("input_format");
    inFormat = inFormat.toLowerCase();

    if (!inFormat.equals("avro") && !inFormat.equals("fastq")) {
      InvalidParameter item = new InvalidParameter(
          "input_format", "input_format must be avro or fastq.");
      items.add(item);
    }
    return items;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AssembleContigs(), args);
    System.exit(res);
  }
}
