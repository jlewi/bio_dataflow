package contrail.stages;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
public class CompressChains extends Stage {
  // TODO(jlewi): Should we create a separate base class for jobs which
  // run several map reduce jobs.
  private static final Logger sLogger = Logger.getLogger(CompressChains.class);

  private long job_start_time = 0;

  // This will be the path to the final graph. The location depends on whether
  // the graph was compressible. If the graph wasn't compressible then
  // this will just ge the input.
  private String finalGraphPath;

  /**
   * Utility function for logging a message when starting a job.
   * @param desc
   */
  private void logStartJob(String desc) {
    sLogger.info(desc + ":\t");

    // TODO(jlewi): What to do about the following variables.
    job_start_time = System.currentTimeMillis();
  }

  public void logEndJob(RunningJob job) throws IOException {
    long endtime = System.currentTimeMillis();
    long diff = (endtime - job_start_time) / 1000;

    sLogger.info(job.getJobID() + " " + diff + " s");

    if (!job.isSuccessful())
    {
      System.out.println("Job was not successful");
      System.exit(1);
    }
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
    compress.setConf(this.getConf());

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
    // to continue from.
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
      RunningJob job = compress.runJob();
      compressible = counter(job, GraphCounters.compressible_nodes);
      logEndJob(job);

      if (compressible == 0) {
        sLogger.info("The graph isn't compressible.");
        finalGraphPath = input_path;
        return;
      }
    }

    sLogger.info("  " + compressible + " compressible\n");
    long lastremaining = compressible;

    while (lastremaining > 0) {
      stage++;

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

      if (lastremaining < LOCALNODES) {
        QuickMarkAvro qmark   = new QuickMarkAvro();
        QuickMergeAvro qmerge = new QuickMergeAvro();
        qmark.setConf(this.getConf());
        qmerge.setConf(this.getConf());

        // Send all the compressible nodes aFile old_path_file = new File(old_path);nd their neighbors to the same
        // machine so they can be compressed in one shot.
        logStartJob("  QMark " + stage);

        Map<String, Object> substage_options =
            ContrailParameters.extractParameters(
                this.stage_options, qmark.getParameterDefinitions().values());

        substage_options.put("inputpath", mark_input);
        substage_options.put("outputpath", marked_graph_path);
        qmark.setParameters(substage_options);
        RunningJob qmark_job = qmark.runJob();
        logEndJob(qmark_job);

        sLogger.info(
            String.format(
                "Nodes to send to compressor: %d \n",
                counter(
                    qmark_job,
                    GraphCounters.quick_mark_nodes_send_to_compressor)));

        logStartJob("  QMerge " + stage);


        Map<String, Object> qmerge_options =
            ContrailParameters.extractParameters(
                this.stage_options, qmerge.getParameterDefinitions().values());

        qmerge_options.put("inputpath", marked_graph_path);
        qmerge_options.put("outputpath", merged_graph_path);
        qmerge.setParameters(qmerge_options);
        RunningJob qmerge_job = qmerge.runJob();
        logEndJob(qmerge_job);

        // Set remaining to zero because all compressible nodes should
        // be compressed.
        remaining = 0;
      }
      else {
        // Use the randomized algorithm
        double rand = Math.random();

        PairMarkAvro pmark   = new PairMarkAvro();
        PairMergeAvro pmerge = new PairMergeAvro();
        pmark.setConf(this.getConf());
        pmerge.setConf(this.getConf());

        {
          logStartJob("Mark" + stage);
          Map<String, Object> mark_options = new HashMap<String, Object>();
          mark_options.put("inputpath", mark_input);
          mark_options.put("outputpath", marked_graph_path);

          Long seed = (long)(rand*10000000);
          mark_options.put("randseed", seed);
          pmark.setParameters(mark_options);
          RunningJob job = pmark.runJob();
          logEndJob(job);

          sLogger.info("  " + counter(job, GraphCounters.num_nodes_to_merge) +
              " marked\n");
        }
        {
          logStartJob("  Merge " + stage);
          Map<String, Object> mark_options = new HashMap<String, Object>();
          mark_options.put("inputpath", marked_graph_path);
          mark_options.put("outputpath", merged_graph_path);
          mark_options.put("K", stage_options.get("K"));
          pmerge.setParameters(mark_options);
          RunningJob job = pmerge.runJob();
          logEndJob(job);
          remaining = counter(job,GraphCounters.pair_merge_compressible_nodes);
        }
      }

      JobConf job_conf = new JobConf(CompressChains.class);
      FileSystem.get(job_conf).delete(new Path(mark_input), true);
      FileSystem.get(job_conf).delete(new Path(marked_graph_path), true);

      String percchange =
          df.format((lastremaining > 0) ? 100*(remaining - lastremaining) /
              lastremaining : 0);
      sLogger.info("  " + remaining + " remaining (" + percchange + "%)\n");

      lastremaining = remaining;
    }

    sLogger.info("Save result to " + final_path + "\n\n");
    FileHelper.moveDirectoryContents(getConf(), latest_path, final_path);
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
            Boolean.class, new Integer(0));

    for (ParameterDefinition def:
      new ParameterDefinition[] {localnodes, resume, stage_num}) {
      definitions.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(definitions);
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath", "localnodes", "K"};
    checkHasParametersOrDie(required_args);

    String input_path = (String) stage_options.get("inputpath");
    String output_path = (String) stage_options.get("outputpath");
    // TODO(jlewi): Is just appending "temp" to the output path
    // really a good idea?
    String temp_path = new Path(output_path, "temp").toString();
    if (stage_options.containsKey("writeconfig")) {
      // TODO(jlewi): Can we write the configuration for this stage like
      // other stages or do we need to do something special?
      throw new NotImplementedException(
          "Support for writeconfig isn't implemented yet for compresschains");
    } else {
      long starttime = System.currentTimeMillis();
      compressChains(input_path, temp_path, output_path);
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);
      System.out.println("Runtime: " + diff + " s");
    }
    return null;
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
