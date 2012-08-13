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
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphN50StatsData;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphStatsData;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;
import contrail.util.FileHelper;

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
    test.expectedStats.setLengths(new ArrayList<Integer>());
    test.expectedStats.getLengths().add(node.getSequence().size());
    return test;
  }

  private void assertMapperOutput(
      MapTestData test,
      AvroCollectorMock<Pair<Integer, GraphStatsData>> collector) {
    assertEquals(1, collector.data.size());
    Pair<Integer, GraphStatsData> pair = collector.data.get(0);

    // We need to negate the bin because that is what the mapper does
    // to sort the bins in descending order.
    assertEquals(test.expectedBin, -1 * pair.key().intValue());
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
    Random generator = new Random();
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

      stats.setDegreeSum(generator.nextInt(100));
      stats.setCoverageSum(generator.nextDouble()*100);
      testData.inputStats.add(stats);
    }

    // Compute the result data for each contig.
    long degreeSum = 0;
    double coverageSum = 0;

    for (int i = 0; i < numRecords; ++i) {
      GraphN50StatsData n50stats = new GraphN50StatsData();
      List<Integer> contigs =
          contigLengths.subList(0, (i + 1) * numContigsPerRecord);

      n50stats.setLengthSum((long) sumList(contigs));
      n50stats.setMaxLength(contigs.get(0));
      n50stats.setMinLength(contigs.get(contigs.size() - 1));
      n50stats.setNumContigs((long) contigs.size());

      degreeSum += testData.inputStats.get(i).getDegreeSum();
      coverageSum += testData.inputStats.get(i).getCoverageSum();

      n50stats.setMeanDegree(
          (double) degreeSum / (double) n50stats.getLengthSum());
      n50stats.setMeanCoverage(
          coverageSum / (double) n50stats.getLengthSum());

      // Compute the N50 length.
      double n50cutoff = n50stats.getLengthSum() / 2.0;

      int runningSum = 0;
      for (int j = 0; j < contigs.size(); ++j) {
        runningSum += contigs.get(j);
        if (runningSum >= n50cutoff) {
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
    for (int trial = 0; trial < 10;  ++trial) {
      N50StatsTestData testData = createN50StatsTest();
      ArrayList<GraphN50StatsData> outputs =
          computeN50Stats(testData.inputStats.iterator());

      // Test the averages are almost equal and then zero them out.
      assertEquals(testData.outputStats.size(), outputs.size());
      for (int i = 0; i < outputs.size(); ++i) {
        assertEquals(
            testData.outputStats.get(i).getMeanCoverage(),
            outputs.get(i).getMeanCoverage(), 0);

        assertEquals(
            testData.outputStats.get(i).getMeanDegree(),
            outputs.get(i).getMeanDegree(), 0);
        testData.outputStats.get(i).setMeanDegree(0.0);
        testData.outputStats.get(i).setMeanCoverage(0.0);
        outputs.get(i).setMeanDegree(0.0);
        outputs.get(i).setMeanCoverage(0.0);

        // Checking individual items as opposed to the arrays makes it
        // easier to understand discrepencies if they occur.
        assertEquals(testData.outputStats.get(i), outputs.get(i));
      }
    }
  }

  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGATT", 3);

    // Add some tips.
    builder.addEdge("ATT", "TTG", 2);
    builder.addEdge("ATT", "TTC", 2);

    File temp = FileHelper.createLocalTempDir();
    File avroFile = new File(temp, "graph.avro");

    GraphUtil.writeGraphToFile(avroFile, builder.getAllNodes().values());

    // Run it.
    GraphStats stage = new GraphStats();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("topn_contigs", new Integer(5));
    params.put("inputpath", avroFile.toString());

    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());

    stage.setParameters(params);

    // Catch the following after debugging.
    try {
      stage.runJob();
    } catch (Exception exception) {
      exception.printStackTrace();
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
