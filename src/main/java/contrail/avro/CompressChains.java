package contrail.avro;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.TTCCLayout;

import contrail.ContrailConfig;

/**
 * Compress linear chains into single nodes.
 *
 * This stage consists of several mapreduce jobs which perform the following
 *   1. Find nodes which can be compressed together.
 *   2. Compress all nodes together (requires several mapreduce jobs).
 */
public class CompressChains extends Stage {
  // TODO(jlewi): Should we create a separate base class for jobs which
  // run several map reduce jobs.
  private static final Logger sLogger = Logger.getLogger(CompressChains.class);
  private static PrintStream logstream;
  private static DecimalFormat df = new DecimalFormat("0.00");

  long GLOBALNUMSTEPS = 0;
  long JOBSTARTTIME = 0;
  private void configureLogger() {
    // TODO(jlewi): Should this be moved into Stage so we can do it for all
    // stages.
    // Setup to use a file appender
    BasicConfigurator.resetConfiguration();

    TTCCLayout lay = new TTCCLayout();
    lay.setDateFormat("yyyy-mm-dd HH:mm:ss.SSS");

    throw new RuntimeException("Code below needs to be updated.");
//    throw new RuntimeException("We should replace localBasePath with a stage option");
//    FileAppender fa = new FileAppender(
//        lay, ContrailConfig.localBasePath+"contrail.details.log", true);
//    fa.setName("File Appender");
//    fa.setThreshold(Level.INFO);
//    BasicConfigurator.configure(fa);
//
//    throw new RuntimeException("We should replace localBasePath with a stage option");
//    FileOutputStream logfile = new FileOutputStream(
//        ContrailConfig.localBasePath+"contrail.log", true);
//    logstream = new PrintStream(logfile);
//
//    ContrailConfig.printConfiguration();
//
//    // Time stamp
//    DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//    msg("== Starting time " + dfm.format(new Date()) + "\n");
//    long globalstarttime = System.currentTimeMillis();
  }

  public void start(String desc) {
    msg(desc + ":\t");

    // TODO(jlewi): What to do about the following variables.
    JOBSTARTTIME = System.currentTimeMillis();
    GLOBALNUMSTEPS++;
  }

  public void end(RunningJob job) throws IOException
  {
    long endtime = System.currentTimeMillis();
    long diff = (endtime - JOBSTARTTIME) / 1000;

    msg(job.getJobID() + " " + diff + " s");

    if (!job.isSuccessful())
    {
      System.out.println("Job was not successful");
      System.exit(1);
    }
  }

  public static void msg(String msg)
  {
    logstream.print(msg);
    System.out.print(msg);
  }

  /**
   * Maximally compress chains.
   *
   * @param basePath
   * @param startname
   * @param finalname
   * @throws Exception
   */
  private void compressChains(String basePath, String startname, String finalname) throws Exception
  {
    CompressibleAvro compress = new CompressibleAvro();

    // TODO(jlewi) Need to add the code for QuickMarkAvro
    // QuickMarkAvro qmark   = new QuickMarkAvro();
    QuickMergeAvro qmerge = new QuickMergeAvro();

    PairMarkAvro pmark   = new PairMarkAvro();
    PairMergeAvro pmerge = new PairMergeAvro();

    int stage = 0;
    long compressible = 0;

    //RunningJob job = null;

    // TODO(jlewi): How should RESTART_COMPRESS etc... be encoded.
    // One possibility is to encode it as an AVRO record which gets passed
    // via the hadoop job configuration. We could encode it using json
    // so it would be human readable.
    if (ContrailConfig.RESTART_COMPRESS > 0)
    {
      throw new RuntimeException("This code needs to be updated");
//      stage = ContrailConfig.RESTART_COMPRESS;
//      compressible = ContrailConfig.RESTART_COMPRESS_REMAIN;
//
//      msg("  Restarting compression after stage " + stage + ":");
//
//      ContrailConfig.RESTART_COMPRESS = 0;
//      ContrailConfig.RESTART_COMPRESS_REMAIN = 0;
    }
    else {
      // Mark compressible nodes
      start("Compressible");
      Map<String, Object> stage_options = new HashMap<String, Object>();
      stage_options.put("inputpath", basePath + startname);
      stage_options.put("outputpath", basePath + startname + "." + stage);
      compress.setOptionValues(stage_options);
      RunningJob job = compress.runJob();
      compressible = counter(job, GraphCounters.compressible_nodes);
      end(job);
    }

    msg("  " + compressible + " compressible\n");
    long lastremaining = compressible;

    while (lastremaining > 0)
    {
      int prev = stage;
      stage++;

      // Input path for marking nodes to be merged.
      String mark_input  = basePath + startname + "." + Integer.toString(prev);
      // The path containing the graph marked for merging.
      String marked_graph_path = mark_input + ".0";
      // The path for the merged graph.
      String merged_graph_path =
          basePath + startname + "." + Integer.toString(stage);

      long remaining = 0;

      if (lastremaining < ContrailConfig.HADOOP_LOCALNODES)
      {
        throw new RuntimeException("This code needs to be updated");
//        // Send all the compressible nodes to the same machine for serial processing
//        start("  QMark " + stage);
//        job = qmark.run(input, input0);
//        end(job);
//
//        msg("  " + counter(job, "compressibleneighborhood") + " marked\n");
//
//        start("  QMerge " + stage);
//        job = qmerge.run(input0, output);
//        end(job);
//
//        remaining = counter(job, "needcompress");
      }
      else
      {
        // Use the randomized algorithm
        double rand = Math.random();

        {
          start("Mark" + stage);
          Map<String, Object> mark_options = new HashMap<String, Object>();
          mark_options.put("inputpath", mark_input);
          mark_options.put("outputpath", marked_graph_path);

          Integer seed = (int)(rand*10000000);
          mark_options.put("randseed", seed.toString());
          pmark.setOptionValues(mark_options);
          RunningJob job = pmark.runJob();
          end(job);

          msg("  " + counter(job, GraphCounters.num_nodes_to_merge) +
              " marked\n");
        }
        {
          start("  Merge " + stage);
          Map<String, Object> mark_options = new HashMap<String, Object>();
          mark_options.put("inputpath", marked_graph_path);
          mark_options.put("outputpath", merged_graph_path);

          pmerge.setOptionValues(mark_options);
          RunningJob job = pmerge.runJob();
          end(job);
          remaining = counter(job,GraphCounters.pair_merge_compressible_nodes);
        }

      }

      JobConf job_conf = new JobConf(CompressChains.class);
      FileSystem.get(job_conf).delete(new Path(mark_input), true);
      FileSystem.get(job_conf).delete(new Path(marked_graph_path), true);

      String percchange =
          df.format((lastremaining > 0) ? 100*(remaining - lastremaining) /
              lastremaining : 0);
      msg("  " + remaining + " remaining (" + percchange + "%)\n");

      lastremaining = remaining;
    }

    JobConf job_conf = new JobConf(CompressChains.class);
    msg("Save result to " + finalname + "\n\n");
    FileUtil.saveResult(
        job_conf, basePath, startname + "." + stage, finalname);
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
   * Get the options required by this stage.
   */
  protected List<Option> getCommandLineOptions() {
    List<Option> super_options = super.getCommandLineOptions();

    HashMap<String, Option> alloptions = new HashMap<String, Option>();
    for (Option option: super_options) {
      alloptions.put(option.getArgName(), option);
    }

    // We add all the options for the stages we depend on.
    Stage[] substages =
      {new CompressibleAvro(), new QuickMergeAvro(), new PairMarkAvro(),
       new PairMergeAvro()};

    for (Stage stage: substages) {
      List<Option> stage_options = stage.getCommandLineOptions();
      for (Option option: stage_options) {
        if (alloptions.containsKey(option.getArgName())) {
          continue;
        }
        alloptions.put(option.getArgName(), option);
        option.getArgName();
      }
    }

    List<Option> options = new ArrayList<Option>();

    options.addAll(alloptions.values());

    return options;
  }

  @Override
  protected void parseCommandLine(CommandLine line) {
    super.parseCommandLine(line);
    throw new RuntimeException("Need to update this code");
  }

  @Override
  public RunningJob runJob() throws Exception {
//    String[] required_args = {"inputpath", "outputpath", "K"};
//    checkHasOptionsOrDie(required_args);
//
//    String inputPath = (String) stage_options.get("inputpath");
//    String outputPath = (String) stage_options.get("outputpath");
//    long K = (Long)stage_options.get("K");
//
//    sLogger.info(" - input: "  + inputPath);
//    sLogger.info(" - output: " + outputPath);
//    sLogger.info(" - K: " + K);
//
//    JobConf conf = new JobConf(PairMergeAvro.class);
//    conf.setJobName("PairMergeAvro " + inputPath + " " + K);
//
//    initializeJobConfiguration(conf);
//
//    FileInputFormat.addInputPath(conf, new Path(inputPath));
//    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
//
//    NodeInfoForMerge merge_info = new NodeInfoForMerge();
//    Pair<CharSequence, NodeInfoForMerge> map_output =
//        new Pair<CharSequence, NodeInfoForMerge> ("", merge_info);
//
//    CompressibleNodeData compressible_node = new CompressibleNodeData();
//    AvroJob.setInputSchema(conf, merge_info.getSchema());
//    AvroJob.setMapOutputSchema(conf, map_output.getSchema());
//    AvroJob.setOutputSchema(conf, compressible_node.getSchema());
//
//    AvroJob.setMapperClass(conf, PairMergeMapper.class);
//    AvroJob.setReducerClass(conf, PairMergeReducer.class);

    if (stage_options.containsKey("writeconfig")) {
     // writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already
//      Path out_path = new Path(outputPath);
//      if (FileSystem.get(conf).exists(out_path)) {
//        // TODO(jlewi): We should only delete an existing directory
//        // if explicitly told to do so.
//        sLogger.info("Deleting output path: " + out_path.toString() + " " +
//            "because it already exists.");
//        FileSystem.get(conf).delete(out_path, true);
//      }
//
//      long starttime = System.currentTimeMillis();
//      JobClient.runJob(conf);
//      long endtime = System.currentTimeMillis();
//
//      float diff = (float) ((endtime - starttime) / 1000.0);
//
//      System.out.println("Runtime: " + diff + " s");
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PairMergeAvro(), args);
    System.exit(res);
  }
}
