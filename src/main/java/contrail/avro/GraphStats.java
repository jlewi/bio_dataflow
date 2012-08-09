package contrail.avro;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.EdgeDirection;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphStatsData;
import contrail.sequences.DNAStrand;


/**
 * Compute statistics of the contigs.
 *
 * We bin the nodes based on the length of the sequences. For each bin
 * we compute statistics such as a weighted sum of the number of edges,
 * number of sequences, the weighted coverage, etc...
 */
public class GraphStats extends Stage {
  private static final Logger sLogger = Logger.getLogger(GraphStats.class);

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

  private static final int n50contigthreshold = 100;

  // Sequences less than this length are considered really short reads.
  private static final int shortCutoff = 50;

  // These are they keys to use with the records corresponding to really short
  // and short reads.
  //  private static final String SHORT_KEY = "SHORT";
  //  private static final String MEDIUM_KEY = "MEDIUM";

  //
  //  // These are the cutoffs to use when binning the nodes.
  //  private static int [] cutoffs =
  //    {        1,      50,    100,    250,    500,
  //        1000,    5000,  10000,  15000,  20000,
  //       25000,   30000,  35000,  40000,  50000,
  //       75000,  100000, 125000, 150000, 200000,
  //      250000,  500000, 750000, 1000000 };

  /**
   * Assign sequences to bins.
   *
   * Sequences are assigned by binning the length. We use a logarithmic
   * scale with 10 bins per order of magnitude.
   * @param len
   * @return
   */
  protected static int binLength(int len) {
    // TODO(jlewi): Should we force really short reads to the same bin?
    return (int) Math.floor(Math.log10(len) * 10);
  }

  protected static class GraphStatsMapper extends
      AvroMapper<GraphNodeData, Pair<Integer, GraphStatsData>> {
    private GraphStatsData graphStats;
    private GraphNode node;
    Pair<Integer, GraphStatsData> outPair;
    //
    //    OutputCollector<Text,Text> mOutput = null;
    //
    //    private static Set<String> fields = new HashSet<String>();
    //
    //    private static Node node = new Node();
    //
    public void configure(JobConf job) {
      graphStats = new GraphStatsData();
      node = new GraphNode();
      outPair = new Pair<Integer, GraphStatsData>(-1, graphStats);
    }



    public void map(GraphNodeData nodeData,
        AvroCollector<Pair<Integer, GraphStatsData>> collector,
        Reporter reporter) throws IOException {
      node.setData(nodeData);
      int len     = node.getSequence().size();
      int fdegree = node.getEdgeTerminals(
          DNAStrand.FORWARD, EdgeDirection.OUTGOING).size();
      int rdegree = node.getEdgeTerminals(
          DNAStrand.REVERSE, EdgeDirection.OUTGOING).size();
      float cov   = node.getCoverage();
      int bin = binLength(len);

      graphStats.setCount(1L);
      graphStats.setLengthSum(len);
      graphStats.setDegreeSum((fdegree + rdegree) * len);
      graphStats.setCoverageSum((double) (cov * len));

      outPair.key(bin);
      collector.collect(outPair);
    }
  }


  public static class GraphStatsReducer
  extends AvroReducer<Integer, GraphStatsData, GraphStatsData>   {

    private GraphStatsData total;
    public void configure(JobConf job) {
      total = new GraphStatsData();
    }

    public void reduce(int bin, Iterable<GraphStatsData> iterable,
        AvroCollector<GraphStatsData> output, Reporter reporter)
            throws IOException   {

      total.setCount(0L);
      total.setCoverageSum(0.0);
      total.setDegreeSum(0);
      total.setLengthSum(0);

      Iterator<GraphStatsData> iter = iterable.iterator();
      while (iter.hasNext()) {
        GraphStatsData item = iter.next();
        total.setCount(total.getCount() + item.getCount());
        total.setCoverageSum(total.getCoverageSum() + item.getCoverageSum());
        total.setDegreeSum(total.getDegreeSum() + item.getDegreeSum());
        total.setLengthSum(total.getLengthSum() + item.getLengthSum());
      }
      output.collect(total);
    }
    //
    //    public void close() throws IOException
    //    {
    //      if (mOutput != null)
    //      {
    //        if (cnts[0] == 0) { throw new IOException("No contigs"); }
    //
    //        Collections.sort(n50sizes); // ascending sort
    //
    //        //mOutput.collect(new Text(),
    //        //    new Text(String.format("%-11s% 10s% 10s% 13s% 10s% 10s% 10s% 10s\n",
    //        //        "Threshold", "Cnt", "Sum", "Mean", "N50", "N50Cnt", "Deg", "Cov")));
    //
    //        mOutput.collect(new Text("Cutoff"), new Text("Cnt\tSum\tMean\tN50\tN50Cnt\tDeg\tCov"));
    //
    //        long n50sum = 0;

              // I think n50candidates is the number of contigs in this bin.
    //        int n50candidates = n50sizes.size();
    //
    //        // find the largest cutoff with at least 1 contig
    //        int curcutoff = -1;
    //
    //        for (int i = cutoffs.length - 1; i >= 0; i--)
    //        {
    //          if (cnts[i] > 0)
    //          {
    //            curcutoff = i;
    //            break;
    //          }
    //        }
    //
    //        // compute the n50 for each cutoff in descending order
    //        long n50cutoff = sums[curcutoff] / 2;
    //
    //        for (int i = 0; (i < n50candidates) && (curcutoff >= 0); i++)
    //        {
    //          int val = n50sizes.get(n50candidates - 1 - i);
    //          n50sum += val;
    //
    //          if (n50sum >= n50cutoff)
    //          {
    //            n50s[curcutoff]  = val;
    //            n50is[curcutoff] = i+1;
    //
    //            curcutoff--;
    //
    //            while (curcutoff >= 0)
    //            {
    //              n50cutoff = sums[curcutoff] / 2;
    //
    //              if (n50sum >= n50cutoff)
    //              {
    //                n50s[curcutoff]  = val;
    //                n50is[curcutoff] = i+1;
    //
    //                curcutoff--;
    //              }
    //              else
    //              {
    //                break;
    //              }
    //            }
    //          }
    //        }
    //
    //        DecimalFormat df = new DecimalFormat("0.00");
    //
    //        // print stats at each cutoff
    //        for (int i = cutoffs.length - 1; i >= 0; i--)
    //        {
    //          int  t      = cutoffs[i];
    //          long c      = cnts[i];
    //
    //          if (c > 0)
    //          {
    //            long s      = sums[i];
    //            long n50    = n50s[i];
    //            long n50cnt = n50is[i];;
    //
    //            double degree = (double) degs[i] / (double) s;
    //            double cov    = (double) covs[i] / (double) s;
    //
    //            //mOutput.collect(new Text(),
    //            //    new Text(String.format(">%-10s% 10d% 10d%13.02f%10d%10d%10.02f%10.02f",
    //            //        t, c, s, (c > 0 ? s/c : 0.0), n50, n50cnt, degree, cov)));
    //            mOutput.collect(new Text(">" + t),
    //                new Text(c + "\t" + s + "\t" + df.format(c > 0 ? (float) s/ (float) c : 0.0) + "\t" +
    //                    n50 + "\t" + n50cnt + "\t" + df.format(degree) + "\t" + df.format(cov)));
    //          }
    //        }
    //
    //        // print the top N contig sizes
    //        if (n50candidates > 0)
    //        {
    //          mOutput.collect(new Text(""), new Text(""));
    //
    //          long topsum = 0;
    //          for (int i = 0; (i < TOPCNT) && (i < n50candidates); i++)
    //          {
    //            int val = n50sizes.get(n50candidates - 1 - i);
    //            topsum += val;
    //            int j = i+1;
    //
    //            mOutput.collect(new Text("max_" + j + ":"), new Text(val + "\t" + topsum));
    //          }
    //        }
    //
    //        // compute the N50 with respect to user specified genome size
    //        if (N50_TARGET > 0)
    //        {
    //          mOutput.collect(new Text(""), new Text(""));
    //          mOutput.collect(new Text("global_n50target:"), new Text(Long.toString(N50_TARGET)));
    //
    //          n50sum = 0;
    //          n50cutoff = N50_TARGET/2;
    //          boolean n50found = false;
    //
    //          for (int i = 0; i < n50candidates; i++)
    //          {
    //            int val = n50sizes.get(n50candidates - 1 - i);
    //            n50sum += val;
    //
    //            if (n50sum >= n50cutoff)
    //            {
    //              int n50cnt = i + 1;
    //              n50found = true;
    //
    //              mOutput.collect(new Text("global_n50:"),    new Text(Integer.toString(val)));
    //              mOutput.collect(new Text("global_n50cnt:"), new Text(Integer.toString(n50cnt)));
    //
    //              break;
    //            }
    //          }
    //
    //          if (!n50found)
    //          {
    //            mOutput.collect(new Text("global_n50:"),    new Text("<" + n50contigthreshold));
    //            mOutput.collect(new Text("global_n50cnt:"), new Text(">" + n50candidates));
    //          }
    //        }
    //      }
    //    }
  }

  //  public RunningJob run(String inputPath, String outputPath) throws Exception
  //  {
  //    sLogger.info("Tool name: Stats");
  //    sLogger.info(" - input: "  + inputPath);
  //    sLogger.info(" - output: " + outputPath);
  //
  //    JobConf conf = new JobConf(Stats.class);
  //    conf.setJobName("Stats " + inputPath);
  //
  //    ContrailConfig.initializeConfiguration(conf);
  //    conf.setNumReduceTasks(1);
  //
  //    FileInputFormat.addInputPath(conf, new Path(inputPath));
  //    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
  //
  //    conf.setInputFormat(TextInputFormat.class);
  //    conf.setOutputFormat(TextOutputFormat.class);
  //
  //    conf.setMapOutputKeyClass(Text.class);
  //    conf.setMapOutputValueClass(Text.class);
  //
  //    conf.setOutputKeyClass(Text.class);
  //    conf.setOutputValueClass(Text.class);
  //
  //    conf.setMapperClass(StatsMapper.class);
  //    conf.setReducerClass(StatsReducer.class);
  //
  //    //delete the output directory if it exists already
  //    FileSystem.get(conf).delete(new Path(outputPath), true);
  //
  //    return JobClient.runJob(conf);
  //  }
  //

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), PairMergeAvro.class);
    } else {
      conf = new JobConf(PairMergeAvro.class);
    }

    conf.setJobName("GraphStats " + inputPath);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));


    Pair<Integer, GraphStatsData> mapOutput =
        new Pair<Integer, GraphStatsData> (0, new GraphStatsData());

    GraphNodeData nodeData = new GraphNodeData();
    AvroJob.setInputSchema(conf, nodeData.getSchema());
    AvroJob.setMapOutputSchema(conf, mapOutput.getSchema());
    AvroJob.setOutputSchema(conf, mapOutput.value().getSchema());

    AvroJob.setMapperClass(conf, GraphStatsMapper.class);
    AvroJob.setCombinerClass(conf, GraphStatsReducer.class);
    AvroJob.setReducerClass(conf, GraphStatsReducer.class);

    // Use a single reducer task that we accumulate all the stats in one
    // reducer.
    conf.setNumReduceTasks(1);

    if (stage_options.containsKey("writeconfig")) {
      writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already
      Path out_path = new Path(outputPath);
      if (FileSystem.get(conf).exists(out_path)) {
        // TODO(jlewi): We should only delete an existing directory
        // if explicitly told to do so.
        sLogger.info("Deleting output path: " + out_path.toString() + " " +
            "because it already exists.");
        FileSystem.get(conf).delete(out_path, true);
      }

      long starttime = System.currentTimeMillis();
      RunningJob job = JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);
      System.out.println("Runtime: " + diff + " s");
      return job;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphStats(), args);
    System.exit(res);
  }
}
