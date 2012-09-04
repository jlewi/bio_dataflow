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
// Author: Michael Schatz, Jeremy Lewi (jeremy@lewi.us)

package contrail.stages;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
import contrail.graph.GraphN50StatsData;
import contrail.sequences.DNAStrand;
import contrail.util.AvroFileContentsIterator;

/**
 * Compute statistics of the contigs.
 *
 * We divide the contigs into bins based on the contig lengths such that
 * each bin contains the contigs with length [L_i, L_i+1) where L_i are spaced
 * logarithmically. For each bin we construct a list of the contig lengths
 * sorted in descending order. We also keep track of some sufficient statistics
 * such as the sum of all the lengths in that bin.
 *
 * The map reduce job is used to organize the contig lengths into bins and
 * to sort the bins into descending order with respect to contig lengths.
 *
 * After the map reduce job completes we process the map reduce output and
 * compute various statistics. The most important statistic being the N50
 * length. The N50 length is the length such that the sum of all contigs
 * greater than or equal to the N50 length is 50% of the sum of the lengths of
 * all contigs.
 *
 * We compute the N50 statistics for each bin. For each bin, the N50 statistics
 * are computed with respect to all contigs in that bin and the bins containing
 * longer contigs. In other words, for each bin i, we compute the N50
 * statistics using all contigs longer than L_i.
 *
 * The N50 stats are written to a separate avro file named "n50stats.avro"
 * in the output directory.
 *
 * If the option "topn_contigs" is given, then the lengths of the N largest
 * contigs will be outputted to the file "topn_contigs.avro" in the output
 * directory as well.
 */
public class GraphStats extends Stage {
  private static final Logger sLogger = Logger.getLogger(GraphStats.class);

  /**
   * Get the parameters used by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    ParameterDefinition topN =
        new ParameterDefinition("topn_contigs",
            "If set to an integer greater than zero then the lengths of the  " +
            "top N contigs will be outputted. ",
            Integer.class, new Integer(0));
    definitions.put(topN.getName(), topN);

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      definitions.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(definitions);
  }

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

    public void configure(JobConf job) {
      graphStats = new GraphStatsData();
      node = new GraphNode();
      outPair = new Pair<Integer, GraphStatsData>(-1, graphStats);
      graphStats.setLengths(new ArrayList<Integer>());

      // Add a single item. Each call to map will overwrite this value.
      graphStats.getLengths().add(0);
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
      graphStats.getLengths().set(0, len);
      graphStats.setLengthSum(len);
      graphStats.setDegreeSum((fdegree + rdegree) * len);
      graphStats.setCoverageSum((double) (cov * len));

      // The output key is the negative of the bin index so that we
      // sort the bins in descending order.
      outPair.key(-1 * bin);
      collector.collect(outPair);
    }
  }

  /**
   * Merge two lists sorted in descending order. The result is in descending
   * order.
   *
   */
  protected static ArrayList<Integer> mergeSortedListsDescending(
      List<Integer> left, List<Integer> right) {
    ArrayList<Integer> merged = new ArrayList<Integer>();
    int left_index = 0;
    int right_index = 0;
    while ((left_index < left.size()) && (right_index < right.size())) {
      if (left.get(left_index) > right.get(right_index)) {
        merged.add(left.get(left_index));
        ++left_index;
      } else {
        merged.add(right.get(right_index));
        ++right_index;
      }
    }

    if (left_index < left.size()) {
      for (; left_index < left.size(); ++left_index) {
        merged.add(left.get(left_index));
      }
    }

    if (right_index < right.size()){
      for (; right_index < right.size(); ++right_index) {
        merged.add(right.get(right_index));
      }
    }
    return merged;
  }

  /**
   * Sum the data in graphstats data.
   *
   * @param iter: Iterator over the graphstats data.
   * @param total: Result. This is zeroed out before summing the data
   */
  protected static void sumGraphStats(
      Iterator<GraphStatsData> iter, GraphStatsData total) {
    total.setCount(0L);
    total.setCoverageSum(0.0);
    total.setDegreeSum(0);
    total.setLengthSum(0);
    total.getLengths().clear();

    while (iter.hasNext()) {
      GraphStatsData item = iter.next();
      total.setCount(total.getCount() + item.getCount());

      ArrayList<Integer> merged = mergeSortedListsDescending(
          total.getLengths(), item.getLengths());

      total.setLengths(merged);
      total.setCoverageSum(total.getCoverageSum() + item.getCoverageSum());
      total.setDegreeSum(total.getDegreeSum() + item.getDegreeSum());
      total.setLengthSum(total.getLengthSum() + item.getLengthSum());
    }
  }

  public static class GraphStatsCombiner
    extends AvroReducer<Integer, GraphStatsData,
                        Pair<Integer, GraphStatsData>> {

    private Pair<Integer, GraphStatsData> outPair;
    private GraphStatsData total;
    public void configure(JobConf job) {
      total = new GraphStatsData();
      total.setLengths(new ArrayList<Integer>());
      outPair = new Pair<Integer, GraphStatsData>(0, total);
    }

    @Override
    public void reduce(
        Integer bin, Iterable<GraphStatsData> iterable,
        AvroCollector<Pair<Integer, GraphStatsData>> output, Reporter reporter)
            throws IOException   {
      Iterator<GraphStatsData> iter = iterable.iterator();
      sumGraphStats(iter, total);
      outPair.key(bin);
      output.collect(outPair);
    }
  }

  public static class GraphStatsReducer
      extends AvroReducer<Integer, GraphStatsData, GraphStatsData> {

    private GraphStatsData total;
    public void configure(JobConf job) {
      total = new GraphStatsData();
      total.setLengths(new ArrayList<Integer>());
    }

    @Override
    public void reduce(Integer bin, Iterable<GraphStatsData> iterable,
        AvroCollector<GraphStatsData> output, Reporter reporter)
            throws IOException   {
      sumGraphStats(iterable.iterator(), total);
      output.collect(total);
    }
  }

  /**
   * Compute the statistics given the data for each bin.
   *
   * This function runs after the mapreduce stage has completed. It takes
   * as input the sufficient statistics for each bin and computes the final
   * statistics.
   *
   * @param iterator: An iterator over the GraphStatsData where each
   *   GraphStatsData contains the data for a different bin. The bins
   *   should be sorted in descending order with respect to the lengths
   *   of the contigs.
   */
  protected static ArrayList<GraphN50StatsData> computeN50Stats(
      Iterator<GraphStatsData> binsIterator) {
    // The output is an array of GraphN50StatsData. Each record gives the
    // N50 stats for a different bin.
    ArrayList<GraphN50StatsData> outputs = new ArrayList<GraphN50StatsData>();

    // Keep a running sum of the lengths across bins.
    long binLengthsSum = 0;

    // Keep a running total of the weighted coverage and degree.
    double binCoverageSum = 0;
    double binDegreeSum = 0;

    // A list of the lengths of the contigs in descending order.
    // This allows us to find the N50 length.
    ArrayList<Integer> contigLengths = new ArrayList<Integer>();

    // This is the index into contigLengths where we continue summing.
    Integer contigIndex = -1;

    // Keep track of  sum(contigLengths[0],..., contigLengths[contigIndex]).
    long contigSum = 0;

    while (binsIterator.hasNext()) {
      GraphStatsData binData = binsIterator.next();
      if (outputs.size() > 0) {
        // Make sure we are sorted in descending order.
        GraphN50StatsData lastBin = outputs.get(outputs.size() - 1);

        if (binData.getLengths().get(0) > lastBin.getMinLength()) {
          throw new RuntimeException(
              "The bins aren't sorted in descending order with respect to " +
              "the contig lengths.");
        }
      }

      contigLengths.addAll(binData.getLengths());
      binLengthsSum += binData.getLengthSum();

      binCoverageSum += binData.getCoverageSum();
      binDegreeSum += (double) binData.getDegreeSum();

      // Compute the N50 length for this value.
      double N50Length = binLengthsSum / 2.0;

      // Continue iterating over the sequences in descending order until
      // we reach a sequence such that the running sum is >= N50Length.
      while (contigSum < N50Length) {
        ++contigIndex;
        contigSum += contigLengths.get(contigIndex);
      }

      // So at this point contigIndex corresponds to the index of the N50
      // cutoff.
      GraphN50StatsData n50Data = new GraphN50StatsData();
      n50Data.setN50Length(contigLengths.get(contigIndex));
      n50Data.setMaxLength(contigLengths.get(0));
      n50Data.setMinLength(contigLengths.get(contigLengths.size() - 1));
      n50Data.setLengthSum(binLengthsSum);
      n50Data.setNumContigs((long) contigLengths.size());
      n50Data.setN50Index(contigIndex);
      n50Data.setMeanCoverage(binCoverageSum / binLengthsSum);
      n50Data.setMeanDegree(binDegreeSum / binLengthsSum);
      outputs.add(n50Data);
    }

    return outputs;
  }

  /**
   * Create an iterator to iterate over the output of the MR job.
   * @return
   */
  protected AvroFileContentsIterator<GraphStatsData> createOutputIterator() {
    String outputDir = (String) stage_options.get("outputpath");
    ArrayList<String> files = new ArrayList<String>();
    FileSystem fs = null;
    try{
      Path outputPath = new Path(outputDir);
      fs = FileSystem.get(this.getConf());
      for (FileStatus status : fs.listStatus(outputPath)) {
        String fileName = status.getPath().getName();
        if (fileName.startsWith("part-") && fileName.endsWith("avro")) {
          files.add(status.getPath().toString());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }
    return new AvroFileContentsIterator<GraphStatsData>(files, getConf());
  }

  /**
   * Get the lengths of the top N contigs.
   *
   * @param iterator: An iterator over the GraphStatsData where each
   *   GraphStatsData contains the data for a different bin. The bins
   *   should be sorted in descending order with respect to the lengths
   *   of the contigs.
   * @param topN: number of sequences to return.
   */
  protected List<Integer> topNContigs(
      Iterator<GraphStatsData> binsIterator, int topN) {
    ArrayList<Integer> outputs = new ArrayList<Integer>();

    while (binsIterator.hasNext() && (outputs.size() < topN)) {
      GraphStatsData binData = binsIterator.next();
      Iterator<Integer> contigIterator = binData.getLengths().iterator();
      int lastLength = Integer.MAX_VALUE;
      while (contigIterator.hasNext() && (outputs.size() < topN)) {
        int length = contigIterator.next();
        if (length > lastLength) {
          throw new RuntimeException(
              "The bins aren't sorted in descending order with respect to " +
              "the contig lengths.");
        }
        outputs.add(length);
        lastLength = length;
      }
    }
    return outputs;
  }

  protected void writeN50StatsToFile(ArrayList<GraphN50StatsData> records) {
    String outputDir = (String) stage_options.get("outputpath");
    Path outputPath = new Path(outputDir, "n50stats.avro");

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    // Write the data to the file.
    Schema schema = records.get(0).getSchema();
    DatumWriter<GraphN50StatsData> datumWriter =
        new SpecificDatumWriter<GraphN50StatsData>(schema);
    DataFileWriter<GraphN50StatsData> writer =
        new DataFileWriter<GraphN50StatsData>(datumWriter);

    try {
      FSDataOutputStream outputStream = fs.create(outputPath);
      writer.create(schema, outputStream);
      for (GraphN50StatsData stats: records) {
        writer.append(stats);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the N50 stats to an avro file. " +
          "Exception: " + exception.getMessage());
    }
  }

  protected void writeTopNContigs(List<Integer> lengths) {
    String outputDir = (String) stage_options.get("outputpath");
    Path outputPath = new Path(outputDir, "topn_contigs.avro");

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    // Write the data to the file.
    Schema schema = Schema.create(Schema.Type.INT);
    DatumWriter<Integer> datumWriter =
        new SpecificDatumWriter<Integer>(schema);
    DataFileWriter<Integer> writer =
        new DataFileWriter<Integer>(datumWriter);

    try {
      FSDataOutputStream outputStream = fs.create(outputPath);
      writer.create(schema, outputStream);
      for (Integer record: lengths) {
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the top N lengths to an avro file. " +
          "Exception: " + exception.getMessage());
    }
  }

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
    AvroJob.setCombinerClass(conf, GraphStatsCombiner.class);
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


      // Create iterators to read the output
      Iterator<GraphStatsData> binsIterator = createOutputIterator();

      // Compute the N50 stats for each bin.
      ArrayList<GraphN50StatsData> N50Stats = computeN50Stats(binsIterator);

      // Write the N50 stats to a file.
      writeN50StatsToFile(N50Stats);

      Integer topn_contigs = (Integer) stage_options.get("topn_contigs");
      if (topn_contigs > 0) {
        // Get the lengths of the n contigs.
        binsIterator = createOutputIterator();
        List<Integer> topN = topNContigs(binsIterator, topn_contigs);
        writeTopNContigs(topN);
      }
      return job;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphStats(), args);
    System.exit(res);
  }
}
