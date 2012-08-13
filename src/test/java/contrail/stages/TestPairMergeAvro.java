// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;

// Extend PairMergeAvro so we can access the mapper and reducer.
public class TestPairMergeAvro extends PairMergeAvro {
  // This class serves as a container for the data for testing the mapper.
  private static class MapperTestCase {
    public MapperTestCase() {
      input = new NodeInfoForMerge();
    }
    // The input to the mapper.
    public NodeInfoForMerge input;

    // The expected key for the mapper output.
    public String key;
  }

  private MapperTestCase mapperNoMergeTest() {
    // Construct a map test case where a node gets sent to itself
    // because it isn't merged.
    MapperTestCase test_case = new MapperTestCase();
    GraphNode node = new GraphNode();
    node.setNodeId("some_node");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    compressible_node.setNode(node.getData());
    compressible_node.setCompressibleStrands(CompressibleStrands.NONE);

    test_case.input.setCompressibleNode(compressible_node);
    test_case.input.setStrandToMerge(CompressibleStrands.NONE);
    test_case.key = node.getNodeId();

    return test_case;
  }

  private MapperTestCase mapperMergeTest() {
    // Construct a map test case where a node gets merged along its reverse
    // strand. So the output is sent to the other terminal.
    MapperTestCase test_case = new MapperTestCase();
    GraphNode node = new GraphNode();
    node.setNodeId("some_node");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));

    EdgeTerminal terminal = new EdgeTerminal("terminal", DNAStrand.REVERSE);
    node.addOutgoingEdge(DNAStrand.REVERSE, terminal);

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    compressible_node.setNode(node.getData());
    compressible_node.setCompressibleStrands(CompressibleStrands.BOTH);

    test_case.input.setCompressibleNode(compressible_node);
    test_case.input.setStrandToMerge(CompressibleStrands.REVERSE);
    test_case.key = terminal.nodeId;

    return test_case;
  }

  // Check the output of the mapper matches the expected result.
  private void assertMapperOutput(
      MapperTestCase test_case,
      AvroCollectorMock<Pair<CharSequence, NodeInfoForMerge>> collector_mock) {
    assertEquals(1, collector_mock.data.size());
    Pair<CharSequence, NodeInfoForMerge> out_pair = collector_mock.data.get(0);
    assertEquals(test_case.key, out_pair.key().toString());
    assertEquals(test_case.input, out_pair.value());
  }

  @Test
  public void testMapper() {
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();
    test_cases.add(mapperNoMergeTest());
    test_cases.add(mapperMergeTest());
    PairMergeMapper mapper = new PairMergeMapper();

    JobConf job = new JobConf(PairMergeMapper.class);

    mapper.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapperTestCase test_case: test_cases) {
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<CharSequence, NodeInfoForMerge>>
        collector_mock =
        new AvroCollectorMock<Pair<CharSequence, NodeInfoForMerge>>();

      try {
        mapper.map(
            test_case.input, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertMapperOutput(test_case, collector_mock);
    }
  }

  // A container class used for organizing the data for the reducer tests.
  private class ReducerTestCase {
    public ReducerTestCase() {
      input = new ArrayList<NodeInfoForMerge>();
      expected_output = new CompressibleNodeData();
    }
    public int K;
    public String reducer_key;
    // The input to the reducer.
    public List<NodeInfoForMerge> input;
    // The expected output from the reducer.
    public CompressibleNodeData expected_output;
  }

  // Asserts that the output of the reducer is correct for this test case.
  private void assertReducerTestCase(
      ReducerTestCase test_case,
      AvroCollectorMock<CompressibleNodeData> collector_mock) {
    assertEquals(1, collector_mock.data.size());
    CompressibleNodeData output = collector_mock.data.get(0);

    // Check the nodes are equal.
    // TODO(jlewi): This might start failing because the order of data
    // in GraphNodeData may not match. We probably need to implement
    // GraphNode.equals
    GraphNode merged_node = new GraphNode(output.getNode());
    GraphNode expected_node =
        new GraphNode(test_case.expected_output.getNode());
    assertEquals(expected_node.getData(), merged_node.getData());
    assertEquals(
        test_case.expected_output.getCompressibleStrands(),
        output.getCompressibleStrands());
  }

  private ReducerTestCase reducerNoMergeTest() {
    // Construct a simple reduce test case in which no nodes are merged.
    ReducerTestCase test_case = new ReducerTestCase();

    test_case.input = new ArrayList<NodeInfoForMerge>();
    test_case.K = 3;
    GraphNode node = new GraphNode();
    node.setNodeId("some_node");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));

    CompressibleNodeData compressible_node = new CompressibleNodeData();
    compressible_node.setNode(node.getData());
    compressible_node.setCompressibleStrands(CompressibleStrands.FORWARD);

    NodeInfoForMerge merge_info = new NodeInfoForMerge();
    merge_info.setCompressibleNode(compressible_node);
    merge_info.setStrandToMerge(CompressibleStrands.NONE);
    test_case.input.add(merge_info);

    test_case.expected_output =
        CompressUtil.copyCompressibleNode(compressible_node);

    return test_case;
  }

  private ReducerTestCase reducerSimpleMergeTest() {
    // Construct a simple reduce test case in which two nodes are merged.
    // TODO(jlewi): We should really randomize this so we cover more cases.
    ReducerTestCase test_case = new ReducerTestCase();
    test_case.K = 3;

    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    // We will merge node ACT and CTT
    builder.addKMersForString("ACTT", test_case.K);

    // Add some incoming/outgoing edges so that we have edges that need to
    // be preserved.
    builder.addEdge("CAC", "ACT", test_case.K - 1);
    builder.addEdge("GAC", "ACT", test_case.K - 1);
    builder.addEdge("CTT", "TTA", test_case.K - 1);

    test_case.input = new ArrayList<NodeInfoForMerge>();


    {
      GraphNode node = builder.getNode(builder.findNodeIdForSequence("ACT"));
      CompressibleNodeData merge_data = new CompressibleNodeData();
      merge_data.setCompressibleStrands(CompressibleStrands.FORWARD);
      merge_data.setNode(node.clone().getData());

      NodeInfoForMerge merge_info = new NodeInfoForMerge();
      merge_info.setCompressibleNode(merge_data);
      merge_info.setStrandToMerge(CompressibleStrands.FORWARD);
      test_case.input.add(merge_info);
    }
    {
      GraphNode node = builder.getNode(builder.findNodeIdForSequence("CTT"));
      CompressibleNodeData merge_data = new CompressibleNodeData();
      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
      merge_data.setNode(node.clone().getData());

      NodeInfoForMerge merge_info = new NodeInfoForMerge();
      merge_info.setCompressibleNode(merge_data);
      merge_info.setStrandToMerge(CompressibleStrands.NONE);
      test_case.input.add(merge_info);
    }

    // Construct the expected output.
    // The down node is CTT. Since the down node preserves its strand
    // the merged node will store AAGT.
    // Which means the merged node will be compressible along the reverse
    // strand.
    GraphNode merged_node = new GraphNode();
    Sequence merged_sequence =
        new Sequence("AAGT", DNAAlphabetFactory.create());
    merged_node.setSequence(merged_sequence);

    String terminal_id = builder.findNodeIdForSequence("TTA");
    merged_node.addOutgoingEdge(
        DNAStrand.REVERSE, new EdgeTerminal(terminal_id, DNAStrand.REVERSE));

    terminal_id = builder.findNodeIdForSequence("CAC");
    merged_node.addOutgoingEdge(
        DNAStrand.FORWARD, new EdgeTerminal(terminal_id, DNAStrand.REVERSE));

    terminal_id = builder.findNodeIdForSequence("GAC");
    merged_node.addOutgoingEdge(
        DNAStrand.FORWARD, new EdgeTerminal(terminal_id, DNAStrand.REVERSE));

    merged_node.setNodeId(builder.findNodeIdForSequence("CTT"));

    test_case.reducer_key = merged_node.getNodeId();
    test_case.expected_output = new CompressibleNodeData();
    test_case.expected_output.setNode(merged_node.getData());
    test_case.expected_output.setCompressibleStrands(
        CompressibleStrands.REVERSE);
    return test_case;
  }

  private ReducerTestCase reducerTwoMergeTest() {
    // Construct a test case where two nodes are merged into another node.
    // i.e we have the chain A->B-C, and nodes A,C get sent to B to be
    // merged.
    ReducerTestCase test_case = new ReducerTestCase();
    test_case.K = 3;

    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    // AAT->ATC>TCT
    builder.addKMersForString("AATCT", test_case.K);

    // Add some incoming/outgoing edges so that we have edges that need to
    // be preserved.
    builder.addEdge("TAA", "AAT", test_case.K - 1);
    builder.addEdge("TCT", "CTT", test_case.K - 1);

    {
      GraphNode node = builder.getNode(builder.findNodeIdForSequence("AAT"));
      CompressibleNodeData merge_data = new CompressibleNodeData();
      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
      merge_data.setNode(node.clone().getData());
      NodeInfoForMerge merge_info = new NodeInfoForMerge();
      merge_info.setCompressibleNode(merge_data);
      merge_info.setStrandToMerge(CompressibleStrands.FORWARD);
      test_case.input.add(merge_info);
    }
    {
      GraphNode node = builder.getNode(builder.findNodeIdForSequence("ATC"));
      CompressibleNodeData merge_data = new CompressibleNodeData();
      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
      merge_data.setNode(node.clone().getData());
      NodeInfoForMerge merge_info = new NodeInfoForMerge();
      merge_info.setCompressibleNode(merge_data);
      merge_info.setStrandToMerge(CompressibleStrands.NONE);
      test_case.input.add(merge_info);
    }
    {
      GraphNode node = builder.getNode(builder.findNodeIdForSequence("TCT"));
      CompressibleNodeData merge_data = new CompressibleNodeData();
      merge_data.setCompressibleStrands(CompressibleStrands.BOTH);
      merge_data.setNode(node.clone().getData());
      NodeInfoForMerge merge_info = new NodeInfoForMerge();
      merge_info.setCompressibleNode(merge_data);
      merge_info.setStrandToMerge(CompressibleStrands.FORWARD);
      test_case.input.add(merge_info);
    }

    // Construct the expected output.
    GraphNode merged_node = new GraphNode();
    // The merged sequence preserves the forward strand of the down node.
    // The down node in this case is "ATC" which is the forward strand
    // so the merged sequence stores AATCT as the forward strand.
    Sequence merged_sequence =
        new Sequence("AATCT", DNAAlphabetFactory.create());
    merged_node.setSequence(merged_sequence);
    merged_node.addIncomingEdge(
        DNAStrand.FORWARD, new EdgeTerminal("TAA", DNAStrand.FORWARD));
    merged_node.addOutgoingEdge(
        DNAStrand.FORWARD, new EdgeTerminal("AAG", DNAStrand.REVERSE));
    merged_node.setNodeId(builder.findNodeIdForSequence("ATC"));

    test_case.reducer_key = merged_node.getNodeId();

    CompressibleNodeData node_output = new CompressibleNodeData();
    node_output.setCompressibleStrands(CompressibleStrands.BOTH);
    node_output.setNode(merged_node.clone().getData());
    test_case.expected_output = node_output;

    return test_case;
  }

  @Test
  public void testReducer() {
    ArrayList<ReducerTestCase> test_cases = new ArrayList<ReducerTestCase>();
    test_cases.add(reducerNoMergeTest());
    test_cases.add(reducerSimpleMergeTest());
    test_cases.add(reducerTwoMergeTest());
    PairMergeReducer reducer = new PairMergeReducer();

    JobConf job = new JobConf(PairMergeReducer.class);

    // TODO: Reduce test cases can only use this value.
    PairMergeAvro stage = new PairMergeAvro();
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("K").addToJobConf(job, new Integer(3));

    reducer.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (ReducerTestCase test_case: test_cases) {
      if (test_case.K != Integer.parseInt(job.get("K"))) {
        job.setLong("K", test_case.K);
        reducer.configure(job);
      }
      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<CompressibleNodeData> collector_mock =
        new AvroCollectorMock<CompressibleNodeData>();

      try {
        reducer.reduce(
            test_case.reducer_key, test_case.input, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertReducerTestCase(test_case, collector_mock);
    }
  }

  @Test
  public void testRun() {
    // This function tests that we can run the job without errors.
    // It doesn't test for correctness.
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();
    test_cases.add(mapperNoMergeTest());
    test_cases.add(mapperMergeTest());

    File temp = null;

    try {
      temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
    }
    if(!(temp.delete())){
        throw new RuntimeException(
            "Could not delete temp file: " + temp.getAbsolutePath());
    }

    if(!(temp.mkdir())) {
        throw new RuntimeException(
            "Could not create temp directory: " + temp.getAbsolutePath());
    }

    File avro_file = new File(temp, "compressible.avro");

    // Write the data to the file.
    Schema schema = (new NodeInfoForMerge()).getSchema();
    DatumWriter<NodeInfoForMerge> datum_writer =
        new SpecificDatumWriter<NodeInfoForMerge>(schema);
    DataFileWriter<NodeInfoForMerge> writer =
        new DataFileWriter<NodeInfoForMerge>(datum_writer);

    try {
      writer.create(schema, avro_file);
      for (MapperTestCase test_case: test_cases) {
        writer.append(test_case.input);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }

    // Run it.
    PairMergeAvro pair_merge = new PairMergeAvro();
    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
       "--K=3"};

    try {
      pair_merge.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
