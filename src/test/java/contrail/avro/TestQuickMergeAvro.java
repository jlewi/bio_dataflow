package contrail.avro;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ContrailConfig;
import contrail.ReporterMock;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;

public class TestQuickMergeAvro {

  @Test
  public void testMap() {
    int ntrials = 10;

    for (int trial = 0; trial < ntrials; trial++) {
      GraphNode node = new GraphNode();
      node.setCanonicalSequence(
          new Sequence("AGTC", DNAAlphabetFactory.create()));
      String readid = "trial_" + trial;
      int chunk = trial*10;
      node.getData().getMertag().setReadTag(readid);
      node.getData().getMertag().setChunk(chunk);
      
      AvroCollectorMock<Pair<CharSequence, GraphNodeData>> collector_mock = 
          new AvroCollectorMock<Pair<CharSequence, GraphNodeData>>();
  
      ReporterMock reporter_mock = new ReporterMock();
      Reporter reporter = (Reporter) reporter_mock;
  
      QuickMergeAvro.QuickMergeMapper mapper = 
          new QuickMergeAvro.QuickMergeMapper();      
      //ContrailConfig.PREPROCESS_SUFFIX = 0;
      ContrailConfig.TEST_MODE = true;
      ContrailConfig.K = 3;
      
      JobConf job = new JobConf(QuickMergeAvro.QuickMergeMapper.class);
      ContrailConfig.initializeConfiguration(job);
      mapper.configure(job);
        
      try {
        mapper.map(
            node.getData(), 
            (AvroCollector<Pair<CharSequence, GraphNodeData>>)collector_mock, 
            reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
      
      // Check the output.
      Iterator<Pair<CharSequence, GraphNodeData>> it = 
          collector_mock.data.iterator();
      Pair<CharSequence, GraphNodeData> pair = it.next();
    
      {
        String expected_key = readid + "_"  + chunk;
        CharSequence output_key = pair.key(); 
        assertEquals(expected_key, output_key.toString());
      }
      // There should be a single output
      assertFalse(it.hasNext());        
    }
  }
  
  @Test
  public void testReducer() {
    // Construct a sequence of nodes to test the merge for.
    // Construct the graph:
    // ATC->TCG->CGC->GCT->CTA
    // CGC->GCC
    // Which after the merge should produce
    // ATCGC->GCTA
    // ATCGC->GCC
    String main_chain = "ATCGCTA";
    int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder(); 
    graph.addKMersForString(main_chain, K);
    
    // Add the edge to break the main chain into two separate chains.
    graph.addEdge("CGC", "GCC", K - 1);
    
    AvroCollectorMock<GraphNodeData> collector_mock = 
        new AvroCollectorMock<GraphNodeData>();

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = (Reporter) reporter_mock;

    QuickMergeAvro.QuickMergeReducer mapper = 
        new QuickMergeAvro.QuickMergeReducer();      
    //ContrailConfig.PREPROCESS_SUFFIX = 0;
    ContrailConfig.TEST_MODE = true;
    ContrailConfig.K = 3;
    
    try {        
      List<GraphNodeData> data = new ArrayList<GraphNodeData>();
      for (GraphNode node: graph.getAllNodes().values()) {
        data.add(node.getData());
      }
      
      QuickMergeAvro.QuickMergeReducer reducer = 
          new QuickMergeAvro.QuickMergeReducer();
      JobConf job = new JobConf(QuickMergeAvro.QuickMergeMapper.class);
      ContrailConfig.initializeConfiguration(job);
      reducer.configure(job);
      
      reducer.reduce("key",
          data, collector_mock, reporter);
    }
    catch (IOException exception){
      fail("IOException occured in reduce: " + exception.getMessage());
    }
    
    Hashtable<String, GraphNode> merged_nodes = 
        new Hashtable<String, GraphNode>();
    
    assertEquals(collector_mock.data.size(), 3);
//    for () {
//      
//    }
    
  }
}
