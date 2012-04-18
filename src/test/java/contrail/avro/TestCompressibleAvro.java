package contrail.avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ContrailConfig;
import contrail.ReporterMock;
import contrail.graph.EdgeDirection;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;

public class TestCompressibleAvro {

  /*
   * Check the output of the map is correct.
   */
  private void assertMapperOutput(
      GraphNodeData expected_node, HashMap<String, CompressibleMessage>
      expected_messages, 
      AvroCollectorMock<Pair<CharSequence, CompressibleMapOutput>> 
          collector_mock) {
    
    // Check the output.
    Iterator<Pair<CharSequence, CompressibleMapOutput>> it = 
        collector_mock.data.iterator();        
    
    boolean has_node = false;
    HashSet<String> has_messages = new HashSet<String>();
    
    while (it.hasNext()) {
      Pair<CharSequence, CompressibleMapOutput> pair = it.next();
      String key = pair.key().toString();
      
      if (key.equals(expected_node.getNodeId().toString())) {
        assertEquals(expected_node, pair.value().getNode());
        assertEquals("Message should not be set", null,
                     pair.value().getMessage());
        has_node = true;
        continue;
      }      
      assertTrue("Message key is invalid", expected_messages.containsKey(key));
      assertEquals(expected_messages.get(key), pair.value().getMessage());
      assertEquals("Node should not be set.", null, pair.value().getNode());
      has_messages.add(key);      
    }
    
    // Check we have the node.
    assertTrue(has_node);
    
    // Check we have all the expected messages.
    assertEquals(expected_messages.keySet(), has_messages);
  }

  // Store the data for a particular test case for the map phase.
  private static class MapTestCaseData {
    public GraphNodeData node;
    public HashMap<String, CompressibleMessage> expected_messages;
  }

  private MapTestCaseData constructMapLinearTestCase() {
    // Construct a linear graph and make sure we output messages
    // containing the nodes and messages to the neighbors.
    String main_chain = "ATCGC";
    int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder(); 
    graph.addKMersForString(main_chain, K);

    // Construct the list of expected messages. This is a hashtable
    // where the key is the id of the destination node.
    HashMap<String, CompressibleMessage> expected_messages = 
        new HashMap<String, CompressibleMessage>();
    
    { 
      CompressibleMessage message = new CompressibleMessage();
      message.setFromDirection(EdgeDirection.OUTGOING);
      message.setFromNodeId("CGA");
      expected_messages.put("ATC", message);
    }
    
    { 
      CompressibleMessage message = new CompressibleMessage();
      message.setFromDirection(EdgeDirection.INCOMING);
      message.setFromNodeId("CGA");
      expected_messages.put("CGC", message);
    }
    
    GraphNode node = graph.getNode("CGA");
    
    MapTestCaseData case_data = new MapTestCaseData();
    case_data.node = node.getData();
    case_data.expected_messages = expected_messages;
    return case_data;
  }
  
  private MapTestCaseData constructMapLinearTestBranching() {
    // Construct a node which has indegree 1 and outdegree 2.
    String main_chain = "CATCG";
    int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder(); 
    graph.addKMersForString(main_chain, K);

    // Add another outgoing edge.
    graph.addEdge("ATC", "TCA", 2);
    
    // Construct the list of expected messages. This is a hashtable
    // where the key is the id of the destination node.
    HashMap<String, CompressibleMessage> expected_messages = 
        new HashMap<String, CompressibleMessage>();    
    { 
      CompressibleMessage message = new CompressibleMessage();
      message.setFromDirection(EdgeDirection.INCOMING);
      message.setFromNodeId("ATC");
            
      expected_messages.put(graph.findNodeIdForSequence("CAT"), message);
    }
    GraphNode node = graph.getNode("ATC");
    
    MapTestCaseData case_data = new MapTestCaseData();
    case_data.node = node.getData();
    case_data.expected_messages = expected_messages;
    return case_data;
  }
   
  @Test
  public void testMap() {
    // Test the mapper. We want to run all the different
    // cases using the same mapper instance as this is more likely to catch
    // issues with static values not being cleared between invocations.            
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = (Reporter) reporter_mock;

    CompressibleAvro.CompressibleMapper mapper = 
        new CompressibleAvro.CompressibleMapper();      
        
    JobConf job = new JobConf(CompressibleAvro.CompressibleMapper.class);

    mapper.configure(job);

    // Construct the different test cases.
    ArrayList<MapTestCaseData> test_cases = new ArrayList<MapTestCaseData>();
    test_cases.add(constructMapLinearTestCase());
    test_cases.add(constructMapLinearTestBranching());
    
    for (MapTestCaseData case_data : test_cases) {
      // We need a new collector for each invocation because the 
      // collector stores the outputs of the mapper.
      AvroCollectorMock<Pair<CharSequence, CompressibleMapOutput>> 
      collector_mock = 
        new AvroCollectorMock<Pair<CharSequence, CompressibleMapOutput>>();
      try {      
        mapper.map(
            case_data.node, 
            (AvroCollector<Pair<CharSequence, CompressibleMapOutput>>)
              collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
            
      assertMapperOutput(
          case_data.node, case_data.expected_messages, collector_mock);
    }
  }
}
