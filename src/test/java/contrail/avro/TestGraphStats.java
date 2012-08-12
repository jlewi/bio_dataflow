package contrail.avro;

import static org.junit.Assert.fail;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphN50StatsData;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphStatsData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

public class TestGraphStats extends GraphStats {
  private static class MapTestData {
    public GraphNodeData input;
    public int expectedBin;
    public GraphStatsData expectedStats;
  }

  private MapTestData createMapTestCase () {
    MapTestData test = new MapTestData();
    GraphNode node = new GraphNode();
    node.setNodeId("1");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));
    node.addOutgoingEdge(
        DNAStrand.FORWARD, new EdgeTerminal("2", DNAStrand.FORWARD));

    node.setCoverage(5.0F);


    test.input = node.getData();

    test.expectedBin = 6;
    test.expectedStats = new GraphStatsData();
    test.expectedStats.setCount(1L);
    test.expectedStats.setLengthSum(4);
    test.expectedStats.setDegreeSum(4 * 1);
    test.expectedStats.setCoverageSum(5.0 * 4);
    return test;
  }

  private void assertMapperOutput(
      MapTestData test,
      AvroCollectorMock<Pair<Integer, GraphStatsData>> collector) {
    assertEquals(1, collector.data.size());
    Pair<Integer, GraphStatsData> pair = collector.data.get(0);

    assertEquals(test.expectedBin, pair.key().intValue());
    assertEquals(test.expectedStats, pair.value());
  }

  @Test
  public void testMapper() {
    ArrayList<MapTestData> testCases = new ArrayList<MapTestData>();
    testCases.add(createMapTestCase());

    GraphStats.GraphStatsMapper mapper = new GraphStats.GraphStatsMapper();
    JobConf job = new JobConf(GraphStats.GraphStatsMapper.class);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapTestData test: testCases) {
      mapper.configure(job);

      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<Integer, GraphStatsData>>
        collector =
          new AvroCollectorMock<Pair<Integer, GraphStatsData>>();

      try {
        mapper.map(
            test.input, collector, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertMapperOutput(test, collector);
    }
  }

  /**
   * Create a list of random integers in descending order.
   * @param size
   * @return
   */
  private ArrayList<Integer> randomDescendingList(int size) {
    Random generator = new Random();
    ArrayList<Integer> result = new ArrayList<Integer>();
    for (int l = 0; l < size; ++l) {
      int length = generator.nextInt(100) + 1;
      result.add(length);
    }
    // Sort the lengths in descending order.
    Collections.sort(result);
    Collections.reverse(result);
    return result;
  }

  @Test
  public void testMergeSortedListsDescending() {
    Random generator = new Random();
    int numTrials = 10;
    for (int trial = 0; trial < numTrials; ++trial) {
      // Create two lists
      ArrayList<Integer> left = randomDescendingList(generator.nextInt(100));
      ArrayList<Integer> right = randomDescendingList(generator.nextInt(100));
      ArrayList<Integer> sorted =
          GraphStats.mergeSortedListsDescending(left, right);

      ArrayList<Integer> expected = new ArrayList<Integer>();
      expected.addAll(left);
      expected.addAll(right);
      Collections.sort(expected);
      Collections.reverse(expected);
      assertEquals(expected, sorted);
    }
  }

  private class ReducerTestCase {
    public List<GraphStatsData> inputs;
    public GraphStatsData expectedOutput;
  }

  private ReducerTestCase createReducerTest () {
    ReducerTestCase test = new ReducerTestCase();
    test.inputs = new ArrayList<GraphStatsData>();

    int num = 5;
    GraphStatsData total = new GraphStatsData();
    total.setLengths(new ArrayList<Integer>());

    Random generator = new Random();

    for (int i = 0; i < num; ++i) {
      GraphStatsData stats = new GraphStatsData();
      stats.setCount((long) generator.nextInt(100) + 1);

      int lengthSum = 0;
      stats.setLengths(new ArrayList<Integer>());
      for (int l = 0; l < stats.getCount(); ++l) {
        int length = generator.nextInt(100);
        stats.getLengths().add(length);
        lengthSum += length;
      }
      // Sort the lengths in descending order.
      Collections.sort(stats.getLengths());
      Collections.reverse(stats.getLengths());

      stats.setCoverageSum(generator.nextDouble());
      stats.setDegreeSum(generator.nextInt(100) + 1);
      stats.setLengthSum(lengthSum);

      test.inputs.add(stats);

      total.setCount(total.getCount() + stats.getCount());
      total.setCoverageSum(total.getCoverageSum() + stats.getCoverageSum());
      total.setDegreeSum(total.getDegreeSum() + stats.getDegreeSum());
      total.setLengthSum(total.getLengthSum() + stats.getLengthSum());
      total.getLengths().addAll(stats.getLengths());
    }

    // Sort the lengths for the result in descending order.
    Collections.sort(total.getLengths());
    Collections.reverse(total.getLengths());

    test.expectedOutput = total;
    return test;
  }

  private void assertReducerTestCase(
      ReducerTestCase test, AvroCollectorMock<GraphStatsData> collector) {
    assertEquals(1, collector.data.size());
    assertEquals(test.expectedOutput, collector.data.get(0));
  }

  @Test
  public void testReducer() {
    ArrayList<ReducerTestCase> testCases = new ArrayList<ReducerTestCase>();
    testCases.add(createReducerTest());

    JobConf job = new JobConf(GraphStatsReducer.class);
    GraphStatsReducer reducer = new GraphStatsReducer();
    reducer.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (ReducerTestCase test: testCases) {
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<GraphStatsData> collector =
        new AvroCollectorMock<GraphStatsData>();

      try {
        reducer.reduce(
            1, test.inputs, collector, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertReducerTestCase(test, collector);
    }
  }

  private class N50StatsTestData {
    ArrayList<GraphStatsData> inputStats;
    ArrayList<GraphN50StatsData> outputStats;

    public N50StatsTestData() {
      inputStats = new ArrayList<GraphStatsData>();
      outputStats = new ArrayList<GraphN50StatsData>();
    }
  }

  private int sumList(List<Integer> data) {
    int result = 0;
    for (Integer num: data) {
      result += num;
    }
    return result;
  }

  private N50StatsTestData createN50StatsTest() {
    N50StatsTestData testData = new N50StatsTestData();

    int numRecords = 10;
    int numContigsPerRecord = 20;
    // Create the contig lengths
    ArrayList<Integer> contigLengths = randomDescendingList(
        numRecords * numContigsPerRecord);

    // Divide the contigs into records.
    for (int i = 0;  i < numRecords; ++i) {
      GraphStatsData stats = new GraphStatsData();
      stats.setLengths(contigLengths.subList(
          i * numContigsPerRecord, (i + 1) * numContigsPerRecord));
      stats.setLengthSum(sumList(stats.getLengths()));
      stats.setCount((long) numContigsPerRecord);

      testData.inputStats.add(stats);
    }

    // Compute the result data for each contig.
    for (int i = 0; i < numRecords; ++i) {
      GraphN50StatsData n50stats = new GraphN50StatsData();
      List<Integer> contigs =
          contigLengths.subList(0, (i + 1) * numContigsPerRecord);

      n50stats.setLengthSum((long) sumList(contigs));
      n50stats.setMaxLength(contigs.get(0));
      n50stats.setMinLength(contigs.get(contigs.size() - 1));
      n50stats.setNumContigs((long) contigs.size());

      // Compute the N50 length.
      double n50cutoff = n50stats.getLengthSum() / 2.0;

      int runningSum = 0;
      for (int j = 0; j < contigs.size(); ++j) {
        runningSum += contigs.get(j);
        if (runningSum > n50cutoff) {
          n50stats.setN50Index(j);
          n50stats.setN50Length(contigs.get(j));
          break;
        }
      }
      testData.outputStats.add(n50stats);
    }
    return testData;
  }

  @Test
  public void testComputeN50Stats() {
    N50StatsTestData testData = createN50StatsTest();
    ArrayList<GraphN50StatsData> outputs =
        computeN50Stats(testData.inputStats.iterator());

    assertEquals(testData.outputStats, outputs);
  }
}
