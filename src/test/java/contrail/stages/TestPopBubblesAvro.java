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
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;
import contrail.util.FileHelper;

public class TestPopBubblesAvro {

  class ReduceTestCase {
    public List<FindBubblesOutput> inputs;
    public GraphNodeData expectedOutput;

    public ReduceTestCase () {
      inputs = new ArrayList<FindBubblesOutput>();
    }
  }

  private void assertReducerOutput(
      ReduceTestCase testCase, AvroCollectorMock<GraphNodeData> collector ) {
    assertEquals(1, collector.data.size());
    assertEquals(testCase.expectedOutput, collector.data.get(0));
  }

  private ReduceTestCase createReduceTest () {
    ReduceTestCase testCase = new ReduceTestCase();
    // Create a test case in which a node has several edges removed.
    GraphNode node = new GraphNode();
    node.setNodeId("minor");
    node.setSequence(new Sequence("ACTG", DNAAlphabetFactory.create()));
    node.setCoverage(1000);

    testCase.expectedOutput = node.clone().getData();

    float extraCoverage = 0;
    for (int i=0; i < 2; ++i) {
      EdgeTerminal terminal = new EdgeTerminal("pop" + i, DNAStrand.FORWARD);
      node.addOutgoingEdge(DNAStrand.FORWARD, terminal);

      FindBubblesOutput input = new FindBubblesOutput();
      input.setMinorNodeId(node.getNodeId());

      input.setDeletedNeighbors(new ArrayList<CharSequence>());
      input.getDeletedNeighbors().add(terminal.nodeId);

      testCase.inputs.add(input);
    }

    FindBubblesOutput nodeInput = new FindBubblesOutput();
    nodeInput.setNode(node.getData());
    nodeInput.setMinorNodeId("");
    nodeInput.setDeletedNeighbors(new ArrayList<CharSequence>());
    testCase.inputs.add(nodeInput);

    return testCase;
  }

  @Test
  public void test() {
    ArrayList<ReduceTestCase> testCases = new ArrayList<ReduceTestCase>();
    testCases.add(createReduceTest());

    PopBubblesAvro.PopBubblesAvroReducer reducer =
        new PopBubblesAvro.PopBubblesAvroReducer();

    JobConf job = new JobConf(PopBubblesAvro.PopBubblesAvroReducer.class);

    PopBubblesAvro stage = new PopBubblesAvro();
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();

    ReporterMock reporterMock = new ReporterMock();
    Reporter reporter = reporterMock;

    for (ReduceTestCase testCase: testCases) {
      definitions.get("K").addToJobConf(job, new Integer(3));
      reducer.configure(job);

      AvroCollectorMock<GraphNodeData> collectorMock =
        new AvroCollectorMock<GraphNodeData>();

      try {
        reducer.reduce(
            "key", testCase.inputs, collectorMock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      assertReducerOutput(testCase, collectorMock);
    }
  }

  private File createInputFile(File tempDir, List<FindBubblesOutput> inputs) {
    File avroFile = new File(tempDir, "bubbles.avro");

    // Write the data to the file.
    Schema schema = (new FindBubblesOutput()).getSchema();
    DatumWriter<FindBubblesOutput> datumWriter =
        new SpecificDatumWriter<FindBubblesOutput>(schema);
    DataFileWriter<FindBubblesOutput> writer =
        new DataFileWriter<FindBubblesOutput>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (FindBubblesOutput bubble : inputs) {
        writer.append(bubble);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
           exception.getMessage());
    }

    return avroFile;
  }

  @Test
  public void testRun() {
    ReduceTestCase testCase = createReduceTest();
    File tempDir = FileHelper.createLocalTempDir();
    createInputFile(tempDir, testCase.inputs);
    // Run it.
    PopBubblesAvro stage = new PopBubblesAvro();
    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());

    File outputPath = new File(tempDir, "output");

    String[] args =
      {"--inputpath=" + tempDir.toURI().toString(),
       "--outputpath=" + outputPath.toURI().toString()};

    try {
      stage.run(args);
    } catch (Exception exception) {
      exception.printStackTrace();
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
