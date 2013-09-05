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
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.util.FileHelper;

/**
 * Compress linear chains into single nodes.
 *
 * This stage consists of several mapreduce jobs which perform the following
 *   1. Find nodes which can be compressed together.
 *   2. Compress all nodes together (requires several mapreduce jobs).
 *
 * If the number of nodes is above some threshold then we use a randomized
 * algorithm (PairMark & PairMerge) to do several merges in parallel.
 * When the number of compressible nodes drops below a threshold then
 * we send all compressible nodes to a single machine to be serially compressed.
 *
 * Input: The input should be an AVRO file encoding the graph. The records
 * should be GraphNodeData.
 *
 * Alternatively, it is possible to resume compressing stages. In this
 * case the input should be an AVRO file whose records are
 * CompressibleNodeData.
 *
 * Note: Resuming the compression hasn't been rigoursouly tested.
 */
public class CompressChains extends PipelineStage {
  // TODO(jlewi): Should we create a separate base class for jobs which
  // run several map reduce jobs.
  private static final Logger sLogger = Logger.getLogger(CompressChains.class);

  private long job_start_time = 0;

  // This will be the path to the final graph. The location depends on whether
  // the graph was compressible. If the graph wasn't compressible then
  // this will just ge the input.
  private String finalGraphPath;

  // Seeds to use if any for pairmark avro.
  private final ArrayList<Integer> seeds;

  public CompressChains() {
    seeds = new ArrayList<Integer>();
  }

  /**
   * Utility function for logging a message when starting a job.
   * @param desc
   */
  private void logStartJob(String desc) {
    sLogger.info(desc + ":\t");

    // TODO(jlewi): What to do about the following variables.
    job_start_time = System.currentTimeMillis();
  }

  /**
   * Maximally compress chains.
   *
   * @param input_path: The directory to act as input. This could
   *   be an uncompressed graph or it could be the output of PairMergeAvro
   *   if we are continuing a previous compression.
   * @param temp_path: The parent directory for the outputs from each
   *   stage of compression.
   * @param final_path: The directory where the final output should be stored.
   * @throws Exception
   */
  private void compressChains(
      String input_path, String temp_path, String final_path) throws Exception {
    finalGraphPath = final_path;
    CompressibleAvro compress = new CompressibleAvro();
    compress.initializeAsChild(this);

    int stage = 0;
    long compressible = 0;
    DecimalFormat df = new DecimalFormat("0.00");

    // The minimum number of nodes for doing parallel compressions.
    // When the number of nodes drops below this number we send
    // all compressible nodes to a single reducer for compression.
    final int LOCALNODES = (Integer) stage_options.get("localnodes");

    // When formatting the step as a string we want to zero pad it
    DecimalFormat sf = new DecimalFormat("00");

    // Keep track of the path from the latest step as this will be
    // the input to the next step.
    String latest_path = null;

    final boolean RESUME = (Boolean) stage_options.get("resume");

    // TODO(jlewi): To determine the step number we should probably parse the
    // directory rather than just letting the user specify the directory
    // to continue from. We should probably rewrite the resume option
    // so that it uses the StageInfo file to figure out where we were.
    if (RESUME) {
      // We resume Mark/Merge iterations after some previous processing
      // Compressible nodes should already be marked so we don't
      // need to run compression.
      sLogger.info("Restarting compression after stage " + stage + ":");

      // If specified we use the stage option to determine the output directory.
      stage = (Integer) stage_options.get("stage");

      latest_path = (String) stage_options.get("inputpath");
    } else {
      // Mark compressible nodes
      logStartJob("Compressible");

      // Make a shallow copy of the stage options required by the compress
      // stage.
      Map<String, Object> substage_options =
          ContrailParameters.extractParameters(
              this.stage_options, compress.getParameterDefinitions().values());

      substage_options.put("inputpath", input_path);

      latest_path =
          (new File(temp_path, "step_" + sf.format(stage))).getPath();

      substage_options.put("outputpath", latest_path);
      compress.setParameters(substage_options);
      executeChild(compress);
      compressible = counter(compress.job, CompressibleAvro.NUM_COMPRESSIBLE);

      if (compressible == 0) {
        sLogger.info("The graph isn't compressible.");
        finalGraphPath = input_path;
        return;
      }
    }

    sLogger.info("Number of compressible nodes:" + compressible);
    long lastremaining = compressible;

    ArrayList<String> pathsToDelete = new ArrayList<String>();

    while (lastremaining > 0) {
      stage++;
      pathsToDelete.clear();

      // Input path for marking nodes to be merged.
      String mark_input  = latest_path;

      // The directory for this step.
      String step_dir =
          new File(temp_path, "step_" + sf.format(stage)).getPath();
      // The path containing the graph marked for merging.
      String marked_graph_path = new File(step_dir, "marked_graph").getPath();

      // The path for the merged graph.
      String merged_graph_path = new File(step_dir, "merged_graph").getPath();

      latest_path = merged_graph_path;
      long remaining = 0;

      // After each step we will want to delete the path containing the input
      // to the marking step and the output of the marking step.
      pathsToDelete.add(mark_input);
      pathsToDelete.add(marked_graph_path);

      if (lastremaining < LOCALNODES) {
        QuickMarkAvro qmark   = new QuickMarkAvro();
        QuickMergeAvro qmerge = new QuickMergeAvro();
        qmark.initializeAsChild(this);
        qmerge.initializeAsChild(this);

        // Send all the compressible nodes and their neighbors to the same
        // machine so they can be compressed in one shot.
        logStartJob("  QMark " + stage);

        Map<String, Object> substage_options =
            ContrailParameters.extractParameters(
                this.stage_options, qmark.getParameterDefinitions().values());

        substage_options.put("inputpath", mark_input);
        substage_options.put("outputpath", marked_graph_path);
        qmark.setParameters(substage_options);
        executeChild(qmark);

        sLogger.info(
            String.format(
                "Nodes to send to compressor: %d \n",
                counter(
                    qmark.job,
                    GraphCounters.quick_mark_nodes_send_to_compressor)));

        logStartJob("  QMerge " + stage);


        Map<String, Object> qmerge_options =
            ContrailParameters.extractParameters(
                this.stage_options, qmerge.getParameterDefinitions().values());

        qmerge_options.put("inputpath", marked_graph_path);
        qmerge_options.put("outputpath", merged_graph_path);
        qmerge.setParameters(qmerge_options);
        executeChild(qmerge);

        // Set remaining to zero because all compressible nodes should
        // be compressed.
        remaining = 0;
      }
      else {
        // Use the randomized algorithm
        double rand = Math.random();

        PairMarkAvro pmark   = new PairMarkAvro();
        PairMergeAvro pmerge = new PairMergeAvro();
        pmark.initializeAsChild(this);
        pmerge.initializeAsChild(this);

        {
          logStartJob("Mark" + stage);
          Map<String, Object> mark_options = new HashMap<String, Object>();
          mark_options.put("inputpath", mark_input);
          mark_options.put("outputpath", marked_graph_path);

          Long seed = (long)(rand*10000000);
          if (stage -1 < seeds.size()) {
            seed = seeds.get(stage -1).longValue();
          }

          mark_options.put("randseed", seed);
          pmark.setParameters(mark_options);
          executeChild(pmark);

          sLogger.info(
              "Number of nodes marked to compress:" +
              counter(pmark.job, PairMarkAvro.NUM_MARKED_FOR_MERGE));
        }
        {
          logStartJob("  Merge " + stage);
          Map<String, Object> mark_options = new HashMap<String, Object>();
          mark_options.put("inputpath", marked_graph_path);
          mark_options.put("outputpath", merged_graph_path);
          mark_options.put("K", stage_options.get("K"));
          pmerge.setParameters(mark_options);
          executeChild(pmerge);
          remaining = counter(
              pmerge.job, PairMergeAvro.NUM_REMAINING_COMPRESSIBLE);
        }

        if (remaining == 0) {
          String convertedGraphPath = new File(
              step_dir, "converted_graph").getPath();
          CompressibleNodeConverter converter =
              new CompressibleNodeConverter();
          converter.initializeAsChild(this);
          // If the number of remaining nodes is zero. Then we need to convert
          // the graph of CompressibleNode's to GraphData. Ordinarily this
          // would happen automatically in QuickMark + QuickMerge.
          logStartJob("Convert to GraphNode's " + stage);
          Map<String, Object> convert_options = new HashMap<String, Object>();
          convert_options.put("inputpath", merged_graph_path);
          convert_options.put("outputpath", convertedGraphPath);
          converter.setParameters(convert_options);
          executeChild(converter);
          latest_path = convertedGraphPath;
          pathsToDelete.add(merged_graph_path);
        }
      }

      JobConf job_conf = new JobConf(CompressChains.class);
      if ((Boolean) stage_options.get("cleanup")) {
        for (String pathToDelete : pathsToDelete) {
          sLogger.info("Deleting:" + pathToDelete);
          Path toDelete = new Path(pathToDelete);
          toDelete.getFileSystem(job_conf).delete(toDelete, true);
        }
      }

      String percchange =
          df.format((lastremaining > 0) ? 100*(remaining - lastremaining) /
              lastremaining : 0);
      sLogger.info(" Number of compressible nodes remaining:" + remaining +
                   " (" + percchange + "%)\n");

      lastremaining = remaining;
    }

    sLogger.info("Moving graph from: " + latest_path);
    sLogger.info("To: " + final_path);
    FileHelper.moveDirectoryContents(getConf(), latest_path, final_path);
    // Record the fact that for the last substage we moved its output.
    StageInfo lastInfo =
        stageInfo.getSubStages().get(stageInfo.getSubStages().size() - 1);

    StageParameter finalPathParameter = new StageParameter();
    finalPathParameter.setName("outputpath");
    finalPathParameter.setValue(final_path);
    lastInfo.getModifiedParameters().add(finalPathParameter);
  }

  /**
   * Return the value of the specified counter in the job.
   * @param job
   * @param group
   * @param tag
   * @return
   * @throws IOException
   */
  private long counter(RunningJob job, GraphCounters.CounterName counter)
      throws IOException {
    return job.getCounters().findCounter(counter.group, counter.tag).getValue();
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    // We add all the options for the stages we depend on.
    Stage[] substages =
      {new CompressibleAvro(), new QuickMergeAvro(), new PairMarkAvro(),
       new PairMergeAvro()};

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    definitions.remove("randseed");

    ParameterDefinition localnodes =
        new ParameterDefinition("localnodes",
            "If the number of compressible nodes is less than this value " +
            "then all compressible nodes get sent to a single worker " +
            "for compression.",
            Integer.class, new Integer(1000));

    ParameterDefinition resume =
        new ParameterDefinition("resume",
            "Indicates we want to resume compressing a set of nodes. " +
            "The input in this case should be an AVRO file with " +
            "CompressibleNodeData records",
            Boolean.class, new Boolean(false));

    ParameterDefinition stage_num =
        new ParameterDefinition("stage",
            "Should only be specified if resume is true. " +
            "This is an integer indicating the next stage in the " +
            "compression. This is optional and only used to name the " +
            "intermediate output directories.",
            Integer.class, new Integer(0));

    ParameterDefinition seeds =
        new ParameterDefinition("seeds",
            "(optional) a comma separated list of seeds to use for the " +
            "random number generator in pairmark. This is useful mainly " +
            "for debugging to repeat a previous run to see what happened.",
            String.class, "");

    for (ParameterDefinition def:
      new ParameterDefinition[] {localnodes, resume, stage_num, seeds}) {
      definitions.put(def.getName(), def);
    }

    ParameterDefinition cleanup = ContrailParameters.getCleanup();
    definitions.put(cleanup.getName(), cleanup);

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * Check if the seeds were passed in and if they were make sure they are
   * unique.
   */
  private void checkAndParseSeeds() {
    if (((String)stage_options.get("seeds")).length() == 0) {
      sLogger.info(
          "No seeds provided. Seeds will be automatically generated.");
      return;
    }
    String seedsString = (String) stage_options.get("seeds");
    String[] seedsArray = seedsString.split(",");

    HashSet<Integer> uniqueSeeds = new HashSet<Integer>();

    for (String someSeed : seedsArray) {
      seeds.add(Integer.parseInt(someSeed));
      if (uniqueSeeds.contains(seeds.get(seeds.size() - 1))) {
        sLogger.fatal(
            "The seeds aren't unique. The seed:" + someSeed + " appears " +
            "multiple times.", new RuntimeException("Duplicate seeds"));
        System.exit(-1);
      }
    }
  }

  @Override
  protected void stageMain() {
    checkAndParseSeeds();

    String input_path = (String) stage_options.get("inputpath");
    String output_path = (String) stage_options.get("outputpath");
    // TODO(jlewi): Is just appending "temp" to the output path
    // really a good idea?
    String temp_path = new Path(output_path, "temp").toString();

    try {
      compressChains(input_path, temp_path, output_path);
    } catch (Exception e) {
      sLogger.fatal("CompressChains failed.", e);
      System.exit(-1);
    }
  }

  /**
   * This will be the path to the final graph.
   *
   * The location depends on whether the graph was compressible. If the graph
   * wasn't compressible then this will just be the input path.
   * @return
   */
  public String getFinalGraphPath() {
    return finalGraphPath;
  }


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new CompressChains(), args);
    System.exit(res);
  }
}
