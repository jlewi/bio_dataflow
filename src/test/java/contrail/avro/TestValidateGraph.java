package contrail.avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.ValidateEdge;
import contrail.graph.ValidateMessage;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;

public class TestValidateGraph extends ValidateGraph {
  // This class serves as a container for the data for testing the mapper.
  private static class MapperTestCase {
    // The input to the mapper.
    public GraphNodeData input;

    // The expected outputs;
    public HashMap<String, ValidateMessage> outputs;
    public int K;
    public MapperTestCase() {
      outputs = new HashMap<String, ValidateMessage>();
    }
  }

  // Create a basic test for the mapper.
  private MapperTestCase createMapTest() {
    GraphNode node = new GraphNode();
    node.setNodeId("some_node");
    Sequence sequence = new Sequence("ACTGC", DNAAlphabetFactory.create());
    node.setSequence(sequence);

    EdgeTerminal forwardEdge = new EdgeTerminal("forward", DNAStrand.FORWARD);
    EdgeTerminal reverseEdge = new EdgeTerminal("reverse", DNAStrand.REVERSE);

    node.addOutgoingEdge(DNAStrand.FORWARD, forwardEdge);
    node.addOutgoingEdge(DNAStrand.REVERSE, reverseEdge);

    MapperTestCase test = new MapperTestCase();
    test.input = node.clone().getData();
    test.K = 3;
    ValidateMessage nodeMessage = new ValidateMessage();
    nodeMessage.setNode(node.clone().getData());

    test.outputs.put(node.getNodeId(), nodeMessage);

    {
      ValidateMessage forwardMessage = new ValidateMessage();
      ValidateEdge edgeInfo = new ValidateEdge();
      edgeInfo.setSourceId(node.getNodeId());
      edgeInfo.setStrands(StrandsForEdge.FF);

      Sequence overlap = sequence.subSequence(
          sequence.size() - test.K + 1, sequence.size());
      edgeInfo.setOverlap(overlap.toCompressedSequence());
      forwardMessage.setEdgeInfo(edgeInfo);
      test.outputs.put(forwardEdge.nodeId, forwardMessage);
    }

    {
      ValidateMessage message = new ValidateMessage();
      ValidateEdge edgeInfo = new ValidateEdge();
      edgeInfo.setSourceId(node.getNodeId());
      edgeInfo.setStrands(StrandsForEdge.RR);

      Sequence overlap = DNAUtil.reverseComplement(sequence) ;
      overlap = overlap.subSequence(
          sequence.size() - test.K + 1, sequence.size());
      edgeInfo.setOverlap(overlap.toCompressedSequence());
      message.setEdgeInfo(edgeInfo);
      test.outputs.put(reverseEdge.nodeId, message);
    }
    return test;
  }

  private void assertMapperOutput(
      MapperTestCase testCase,
      AvroCollectorMock<Pair<CharSequence, ValidateMessage>> collector) {
    HashMap<String, ValidateMessage> actualOutputs =
        new HashMap<String, ValidateMessage>();
    for (Pair<CharSequence, ValidateMessage> outPair: collector.data) {
      actualOutputs.put(outPair.key().toString(), outPair.value());
    }

    assertEquals(testCase.outputs, actualOutputs);
  }
  @Test
  public void testMapper() {
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();
    test_cases.add(createMapTest());

    ValidateGraphMapper mapper = new ValidateGraphMapper();
    JobConf job = new JobConf(ValidateGraphMapper.class);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (MapperTestCase test_case: test_cases) {
      job.setInt("K", test_case.K);
      mapper.configure(job);

      // We need a new collector for each invocation because the
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<CharSequence, ValidateMessage>>
        collector_mock =
          new AvroCollectorMock<Pair<CharSequence, ValidateMessage>>();

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
}
