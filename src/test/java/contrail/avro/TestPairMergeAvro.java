package contrail.avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeDirection;
import contrail.graph.GraphNode;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAStrand;

// Extend PairMergeAvro so we can access mapper and reducer.
public class TestPairMergeAvro extends PairMergeAvro {

  // A CoinFlipper which is not random but uses a hash table to map
  // strings to coin flips. This makes it easy to control the tosses assigned
  // to the nodes.
  private static class CoinFlipperFixed extends CoinFlipper {
    public HashMap<String, CoinFlip> tosses;
    public CoinFlipperFixed() {
      super(0);
      tosses = new HashMap<String, CoinFlip>();
    }
    
    public CoinFlip flip(String seed) {
      if (!tosses.containsKey(seed)) {
        throw new RuntimeException("Flipper is missing seed:" + seed);
      }
      return tosses.get(seed);
    }
  }
  
  // This class contains the data for testing the mapper.
  private static class MapperTestCase {
    public MapperTestCase() {
      input = new ArrayList<CompressibleNodeData>();
      expected_output = 
          new HashMap<String, Pair<CharSequence, MergeNodeData>>();
      flipper = new CoinFlipperFixed();
    }
    public List<CompressibleNodeData> input;
    public HashMap<String, Pair<CharSequence, MergeNodeData>> expected_output;
    
    // The flipper to use in the test.
    public CoinFlipper flipper;
  }
  
  private MapperTestCase simpleMapperTest() {
    // Construct the simplest mapper test case.
    // We have two nodes. The first is assigned heads and the second tails.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTG", 3);
    
    MapperTestCase test_case = new MapperTestCase();
    for (GraphNode node: builder.getAllNodes().values()) {
      CompressibleNodeData data = new CompressibleNodeData();
      data.setNode(node.getData());
      
      data.setCompressibleStrands(CompressibleStrands.NONE);
      if (node.degree(DNAStrand.FORWARD) == 1 &&  
          node.degree(DNAStrand.REVERSE) == 1) {
        data.setCompressibleStrands(CompressibleStrands.BOTH);
      } else if (node.degree(DNAStrand.FORWARD) == 1) {
        data.setCompressibleStrands(CompressibleStrands.FORWARD);
      } else if (node.degree(DNAStrand.REVERSE) == 1) {
        data.setCompressibleStrands(CompressibleStrands.REVERSE);
      }      
      test_case.input.add(data);
    }
    
    // Make the first node the up node.
    GraphNode up_node = builder.getNode(builder.findNodeIdForSequence("ACT"));
    GraphNode down_node = builder.getNode(builder.findNodeIdForSequence("CTG"));
    
    CoinFlipperFixed flipper = new CoinFlipperFixed();
    test_case.flipper = flipper;
    flipper.tosses.put(up_node.getNodeId(), CoinFlipper.CoinFlip.Up);
    flipper.tosses.put(
        down_node.getNodeId(), CoinFlipper.CoinFlip.Down);
    
    MergeNodeData up_output = new MergeNodeData();
    up_output.setNode(up_node.clone().getData());
    up_output.setStrandToMerge(CompressibleStrands.FORWARD);
    
    // Up node is sent to the down node.
    test_case.expected_output.put(up_node.getNodeId(),
        new Pair<CharSequence, MergeNodeData>(down_node.getNodeId(), up_output));
    
    MergeNodeData down_output = new MergeNodeData();
    down_output.setNode(up_node.clone().getData());
    down_output.setStrandToMerge(CompressibleStrands.NONE);
    
    test_case.expected_output.put(down_node.getNodeId(),
        new Pair<CharSequence, MergeNodeData>(down_node.getNodeId(), down_output));
    return test_case;
  }

  private MapperTestCase mapperNoMergeTest() {
    // Construct the test case where the nodes can't be compressed.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTG", 3);
    builder.addEdge("ACT","CTC", 2);
    
    MapperTestCase test_case = new MapperTestCase();
    for (GraphNode node: builder.getAllNodes().values()) {
      CompressibleNodeData data = new CompressibleNodeData();
      data.setNode(node.getData());
      
      // Nodes aren't compressible.
      data.setCompressibleStrands(CompressibleStrands.NONE);

      MergeNodeData output = new MergeNodeData();
      output.setNode(node.clone().getData());
      output.setStrandToMerge(CompressibleStrands.NONE);
      // Up node is sent to the down node.
      test_case.expected_output.put(node.getNodeId(),
          new Pair<CharSequence, MergeNodeData>(node.getNodeId(), output));
    }

    // Use the random coin flipper.
    test_case.flipper = new CoinFlipper(12);
    return test_case;
  }
  
  // Check the output of the mapper matches the expected result.
  private void assertMapperOutput(
      CompressibleNodeData input, 
      Pair<CharSequence, MergeNodeData> expected_output, 
      AvroCollectorMock<Pair<CharSequence, MergeNodeData>> collector_mock) {    
    assertEquals(1, collector_mock.data.size());
    assertEquals(expected_output, collector_mock.data.get(0));
  }
  
  @Test
  public void testMapper() {
    ArrayList<MapperTestCase> test_cases = new ArrayList<MapperTestCase>();    
    test_cases.add(mapperNoMergeTest());
    test_cases.add(simpleMapperTest());
    
    PairMergeMapper mapper = new PairMergeMapper();      
        
    JobConf job = new JobConf(PairMergeMapper.class);
    job.setLong("randseed", 11);
    
    mapper.configure(job);

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = (Reporter) reporter_mock;
    
    for (MapperTestCase test_case: test_cases) {          
      for (CompressibleNodeData input_data: test_case.input) {
        // We need a new collector for each invocation because the 
        // collector stores the outputs of the mapper.
        AvroCollectorMock<Pair<CharSequence, MergeNodeData>> collector_mock = 
          new AvroCollectorMock<Pair<CharSequence, MergeNodeData>>();
        
        mapper.setFlipper(test_case.flipper);
        try {                
          mapper.map(
              input_data, 
              (AvroCollector<Pair<CharSequence, MergeNodeData>>)
                collector_mock, reporter);
        }
        catch (IOException exception){
          fail("IOException occured in map: " + exception.getMessage());
        }
              
        assertMapperOutput(
            input_data, 
            test_case.expected_output.get(input_data.getNode().getNodeId()), 
            collector_mock);
      }
    }           
  }
}
