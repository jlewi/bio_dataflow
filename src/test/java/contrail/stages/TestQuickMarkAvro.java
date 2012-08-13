package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.QuickMarkMessage;
import contrail.ReporterMock;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.KMerReadTag;

public class TestQuickMarkAvro extends QuickMarkAvro{

  /*
   * Check the output of the map is correct.
   */
  private void assertMapperOutput(
      CompressibleNodeData expected_node, HashMap<String, QuickMarkMessage> expected_message,
      AvroCollectorMock<Pair<CharSequence, QuickMarkMessage>> collector_mock) {

    // Check the output.
    Iterator<Pair<CharSequence, QuickMarkMessage>> it =
        collector_mock.data.iterator();

    GraphNode temp_node= new GraphNode();
    temp_node.setData(expected_node.getNode());

    while (it.hasNext()) {
      Pair<CharSequence, QuickMarkMessage> pair = it.next();
      String key = pair.key().toString();
      assertEquals(expected_message.get(key), pair.value());
    }
  }

  // Store the data for a particular test case for the map phase.
  private static class MapTestCaseData {
    public CompressibleNodeData node;
    public HashMap<String, QuickMarkMessage> expected_message;	// because mapper may not have single output message
  }

  private List<MapTestCaseData> constructMapCases() {

    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);

    // Construct the list of test cases
    List <MapTestCaseData> cases= new ArrayList<MapTestCaseData>();

    // map output {nodeid, msg< nodedata, has Strands to Compress>}
    // key: TCA 1 message
    // {nodeid(TCA), msg<TCA, true>}
    // {nodeid(AAATC), msg<null, true>}
    {
      MapTestCaseData non_compress= new MapTestCaseData();
      HashMap <String, QuickMarkMessage> expected_non_compress= new HashMap <String, QuickMarkMessage>();

      GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCA"));
      String neighbor_node_Id = graph.getNode(graph.findNodeIdForSequence("AAATC")).getNodeId();

      CompressibleNodeData node_data = new CompressibleNodeData();
      node_data.setNode(node.clone().getData());
      node_data.setCompressibleStrands(CompressibleStrands.REVERSE);

      QuickMarkMessage msg1 = new QuickMarkMessage();
      GraphNode temp_node= new GraphNode();
      temp_node.setData(node_data.getNode());
      msg1.setNode(temp_node.getData());
      msg1.setSendToCompressor(true);

      QuickMarkMessage msg2 = new QuickMarkMessage();
      msg2.setNode(null);
      msg2.setSendToCompressor(true);

      expected_non_compress.put(node.getNodeId(), msg1);
      expected_non_compress.put(neighbor_node_Id, msg2);
      non_compress.node= node_data;
      non_compress.expected_message= expected_non_compress;

      cases.add(non_compress);
    }
    //  key: AAATC 2 messages
    // {nodeid(AAATC), msg<AAATC, true>}
    // {nodeid(TCA), msg<null, true>}
    {
      MapTestCaseData compress= new MapTestCaseData();
      HashMap<String, QuickMarkMessage> expected_compress= new HashMap <String, QuickMarkMessage>();

      GraphNode node = graph.getNode(graph.findNodeIdForSequence("AAATC"));
      String neighbor_node_Id = graph.getNode(graph.findNodeIdForSequence("TCA")).getNodeId();

      CompressibleNodeData node_data = new CompressibleNodeData();
      node_data.setNode(node.clone().getData());
      node_data.setCompressibleStrands(CompressibleStrands.FORWARD);

      GraphNode temp_node= new GraphNode();
      temp_node.setData(node_data.getNode());

      QuickMarkMessage msg1 = new QuickMarkMessage();
      msg1.setNode(temp_node.getData());
      msg1.setSendToCompressor(true);
      expected_compress.put(temp_node.getNodeId(), msg1);


      QuickMarkMessage msg2 = new QuickMarkMessage();
      msg2.setNode(null);
      msg2.setSendToCompressor(true);
      expected_compress.put(neighbor_node_Id, msg2);
      compress.node= node_data;
      compress.expected_message= expected_compress;

      cases.add(compress);
    }

    return cases;
  }


  @Test
  public void testMap() {

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    QuickMarkAvro.QuickMarkMapper mapper = new QuickMarkAvro.QuickMarkMapper();
    JobConf job = new JobConf(QuickMarkAvro.QuickMarkMapper.class);

    mapper.configure(job);

    // Construct the different test cases.
    List <MapTestCaseData> test_cases = constructMapCases();

    for (MapTestCaseData case_data : test_cases) {

      AvroCollectorMock<Pair<CharSequence, QuickMarkMessage>>
      collector_mock =  new AvroCollectorMock<Pair<CharSequence, QuickMarkMessage>>();
      try {
        mapper.map(case_data.node, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertMapperOutput(case_data.node, case_data.expected_message, collector_mock);
    }

  }


  private static class ReduceTestCaseData {
    List <QuickMarkMessage> map_out_list;
    GraphNodeData expected_node_data;
  }
  private void assertReduceOutput(
      ReduceTestCaseData case_data,
      AvroCollectorMock<GraphNodeData> collector_mock) {

    assertEquals(1, collector_mock.data.size());
    assertEquals(case_data.expected_node_data, collector_mock.data.get(0));
  }
  private List<ReduceTestCaseData> constructReduceTips() {
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addEdge("AAATC", "TCA", 2);

    List<ReduceTestCaseData> test_data_list= new ArrayList<ReduceTestCaseData> ();
    List <QuickMarkMessage> map_out_list= new ArrayList <QuickMarkMessage>();
    ReduceTestCaseData test_data= new ReduceTestCaseData();

    List <QuickMarkMessage> map_out_list2= new ArrayList <QuickMarkMessage>();
    ReduceTestCaseData test_data2= new ReduceTestCaseData();

    // Output of the Mapper is in the form {Key, Message <id,Boolean>}; mapper outputs are as follows
    // nodeid(TCA), <TCA,true>
    // nodeid(TCA), <null,true>
    GraphNode temp_node= null;		// node TCA
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("TCA"));
      QuickMarkMessage msg1 = new QuickMarkMessage();
      QuickMarkMessage msg2 = new QuickMarkMessage();

      CompressibleNodeData node_data = new CompressibleNodeData();
      node_data.setNode(node.clone().getData());
      node_data.setCompressibleStrands(CompressibleStrands.REVERSE);

      temp_node= new GraphNode();
      temp_node.setData(node_data.getNode());

      msg1.setNode(temp_node.getData());
      msg1.setSendToCompressor(true);
      msg2.setNode(null);
      msg2.setSendToCompressor(true);

      map_out_list.add(msg1);
      map_out_list.add(msg2);
    }

    KMerReadTag readtag = new KMerReadTag("compress", 0);
    temp_node.setMertag(readtag);
    // both messages are now in a List; we now set the expected node/ key for those msg
    test_data.expected_node_data= temp_node.getData();
    test_data.map_out_list= map_out_list;

    // add key/msg list to Object List
    test_data_list.add(test_data);

    GraphNode temp_node2= null;	// node AAATC
    // nodeid(AAATC), <AAATC,true>
    // nodeid(AAATC), <null, true>}
    {
      GraphNode node = graph.getNode(graph.findNodeIdForSequence("AAATC"));
      QuickMarkMessage msg1 = new QuickMarkMessage();
      QuickMarkMessage msg2 = new QuickMarkMessage();

      CompressibleNodeData node_data = new CompressibleNodeData();
      node_data.setNode(node.clone().getData());
      node_data.setCompressibleStrands(CompressibleStrands.FORWARD);

      temp_node2= new GraphNode();
      temp_node2.setData(node_data.getNode());

      msg1.setNode(temp_node2.getData());
      msg1.setSendToCompressor(true);
      msg2.setNode(null);
      msg2.setSendToCompressor(true);

      map_out_list2.add(msg1);
      map_out_list2.add(msg2);

      KMerReadTag readtag2 = new KMerReadTag("compress", 0);
      temp_node2.setMertag(readtag2);
    }
    test_data2.expected_node_data= temp_node2.getData();
    test_data2.map_out_list= map_out_list2;

    // add key/msg list to Object List
    test_data_list.add(test_data2);
    return test_data_list;
  }
  @Test
  public void testReduce() {
    List <ReduceTestCaseData> case_data_list= constructReduceTips();
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    JobConf job = new JobConf(QuickMarkAvro.QuickMarkReducer.class);
    QuickMarkAvro.QuickMarkReducer reducer = new QuickMarkAvro.QuickMarkReducer();
    reducer.configure(job);

    for (ReduceTestCaseData case_data : case_data_list) {

      AvroCollectorMock<GraphNodeData> collector_mock = new AvroCollectorMock<GraphNodeData>();
      try {
        CharSequence key = case_data.expected_node_data.getNodeId();
        reducer.reduce(key, case_data.map_out_list, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
      assertReduceOutput(case_data, collector_mock);
    }
  }


	@Test
	public void testRun() {
		SimpleGraphBuilder builder = new SimpleGraphBuilder();
		builder.addKMersForString("ACTGG", 3);
		File temp = null;
		try {
			temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
		} catch (IOException exception) {
			fail("Could not create temporary file. Exception:" +exception.getMessage());
		}

		if(!(temp.delete())){
			throw new RuntimeException(
					"Could not delete temp file: " + temp.getAbsolutePath());
		}
		if(!(temp.mkdir())) {
			throw new RuntimeException(
					"Could not create temp directory: " + temp.getAbsolutePath());
		}
		File avro_file = new File(temp, "graph.avro");
		// Write the data to the file.
		Schema schema = (new CompressibleNodeData()).getSchema();
		DatumWriter<CompressibleNodeData> datum_writer =
		    new SpecificDatumWriter<CompressibleNodeData>(schema);
		DataFileWriter<CompressibleNodeData> writer =
		    new DataFileWriter<CompressibleNodeData>(datum_writer);
		try {
			writer.create(schema, avro_file);
			for (GraphNode node: builder.getAllNodes().values()) {
			  CompressibleNodeData compressible_node = new CompressibleNodeData();
			  compressible_node.setNode(node.getData());
			  if (node.getNodeId().equals("ACT")) {
			    compressible_node.setCompressibleStrands(CompressibleStrands.FORWARD);
			  } else if (node.getNodeId().equals("CCA")) {
			    compressible_node.setCompressibleStrands(CompressibleStrands.FORWARD);
			  } else {
			    compressible_node.setCompressibleStrands(CompressibleStrands.BOTH);
			  }
				writer.append(compressible_node);
			}
			writer.close();
		} catch (IOException exception) {
		    fail("There was a problem writing the graph to an avro file. " +
		         "Exception:" + exception.getMessage());
		}
		// Run it.
		QuickMarkAvro run_quickmark = new QuickMarkAvro();
		File output_path = new File(temp, "output");
		String[] args =
			{"--inputpath=" + temp.toURI().toString(),
			 "--outputpath=" + output_path.toURI().toString(),
			};
		try {
			run_quickmark.run(args);
		} catch (Exception exception) {
			fail("Exception occured:" + exception.getMessage());
		}
	}
}
