package contrail.avro;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.util.FileHelper;

/**
 * This stage iteratively compresses chains and does error correction.
 *
 * Each round of compression can expose new topological errors that can be
 * corrected. Similarly, correcting errors can expose new chains which can
 * be compressed. Consequently, we repeatedly perform compression followed
 * by error correction until we have a round where the graph doesn't change.
 */
public class CompressAndCorrect extends Stage {
  private static final Logger sLogger = Logger.getLogger(CompressChains.class);
  /**
   * Get the parameters used by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    // We add all the options for the stages we depend on.
    Stage[] substages =
      {new CompressChains(), new RemoveTipsAvro()};

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    return Collections.unmodifiableMap(definitions);
  }


  private void compressGraph(String inputPath, String outputPath)
      throws Exception {
    CompressChains compressStage = new CompressChains();
    compressStage.setConf(getConf());
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            compressStage.getParameterDefinitions().values());

    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    compressStage.setParameters(stageOptions);
    RunningJob compressJob = compressStage.runJob();
  }

  /**
   * Remove the tips in the graph.
   * @param inputPath
   * @param outputPath
   * @return True if any tips were removed.
   */
  private boolean removeTips(String inputPath, String outputPath)
      throws Exception {
    RemoveTipsAvro stage = new RemoveTipsAvro();
    stage.setConf(getConf());
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            stage.getParameterDefinitions().values());

    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);
    RunningJob job = stage.runJob();

    // Check if any tips were found.
    long tipsRemoved = job.getCounters().findCounter(
        GraphCounters.remove_tips_tips_removed.group,
        GraphCounters.remove_tips_tips_removed.tag).getValue();

    boolean hadTips = false;
    if (tipsRemoved > 0) {
      hadTips = true;
    }
    return hadTips;
  }

  private void processGraph() throws Exception {
    // TODO(jlewi): Does this function really need to throw an exception?
    String outputPath = (String) stage_options.get("outputpath");

    // Create a subdirectory of the output path to contain the temporary
    // output from each substage.
    String tempPath = new Path(outputPath, "temp").toString();

      int step = 1;

      // When formatting the step as a string we want to zero pad it
      DecimalFormat sf = new DecimalFormat("00");

      // Keep track of the latest input for the step.
      String stepInputPath = (String) stage_options.get("inputpath");
      boolean  done = false;

      // Keep track of the input for the current round.
      //String stepInputPath = input_path;

      while (!done) {
        // Create a subdirectory of the temp directory to contain the output
        // from this round.
        String stepPath = new Path(
            tempPath, "step_" +sf.format(step)).toString();

        // Paths to use for this round. The paths for the compressed graph
        // and the error corrected graph.
        String compressedPath = new Path(
            stepPath, "CompressChains").toString();
        String removeTipsPath = new Path(stepPath, "RemoveTips").toString();

        compressGraph(stepInputPath, compressedPath);
        boolean hadTips = removeTips(compressedPath, removeTipsPath);

        // If no tips were found then we have compressed the graph as much
        // as possible so we are done.
        if (!hadTips) {
          done = true;
        }
        stepInputPath = removeTipsPath;
      }

    sLogger.info("Save result to: " + outputPath + "\n\n");
    FileHelper.moveDirectoryContents(getConf(), stepInputPath, outputPath);

    // Clean up the intermediary directories.
    // TODO(jlewi): We might want to add an option to keep the intermediate
    // directories.
    sLogger.info("Delete temporary directory: " + tempPath + "\n\n");
    FileSystem.get(getConf()).delete(new Path(tempPath), true);
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    if (stage_options.containsKey("writeconfig")) {
      // TODO(jlewi): Can we write the configuration for this stage like
      // other stages or do we need to do something special?
      throw new NotImplementedException(
          "Support for writeconfig isn't implemented yet for " +
          "CompressAndCorrect");
    } else {
      long starttime = System.currentTimeMillis();
      processGraph();
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);
      sLogger.info("Runtime: " + diff + " s");
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CompressAndCorrect(), args);
    System.exit(res);
  }
}
