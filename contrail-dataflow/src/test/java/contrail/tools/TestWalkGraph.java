package contrail.tools;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.util.FileHelper;

// TODO(jeremy@lewi.us) We need to add a unittest which covers the following
// case: We have multiple seeds which we start from. The subgraphs overlap.
// We want to verify that all nodes are outputted at most once even though
// some nodes will be encountered multiple teams since they are reachable
// from both seeds.
public class TestWalkGraph {

  private static class TestCase {
    public String sortedGraphFile;
    public Map<String, GraphNode> nodes;
  }
  private TestCase createTestGraph(String testDir) {
    TestCase testCase = new TestCase();
    testCase.sortedGraphFile = FilenameUtils.concat(testDir, "fullgraph");

    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    Random generator = new Random();
    String sequence =
        AlphabetUtil.randomString(generator, 66, DNAAlphabetFactory.create());
    builder.addKMersForString(sequence, 33);

    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
    nodes.addAll(builder.getAllNodes().values());
    testCase.nodes = builder.getAllNodes();

    // Sort the nodes by key.
    Collections.sort(nodes, new GraphUtil.nodeIdComparator());

    SortedKeyValueFile.Writer.Options writerOptions =
        new SortedKeyValueFile.Writer.Options();

    writerOptions.withConfiguration(new Configuration());
    writerOptions.withKeySchema(Schema.create(Schema.Type.STRING));
    writerOptions.withValueSchema(nodes.get(0).getData().getSchema());
    writerOptions.withPath(new Path(testCase.sortedGraphFile));

    SortedKeyValueFile.Writer<CharSequence, GraphNodeData> writer;

    try {
        writer = new SortedKeyValueFile.Writer<CharSequence, GraphNodeData>(
            writerOptions);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }

    try {
      for (GraphNode node : nodes) {
        writer.append(node.getNodeId(), node.getData());
      }
      writer.close();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
    return testCase;
  }

  @Test
  public void testRun() {
    File temp = FileHelper.createLocalTempDir();

    TestCase testCase = createTestGraph(temp.getPath());

    // Run it.
    WalkGraph stage = new WalkGraph();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", testCase.sortedGraphFile);

    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());
    params.put("num_hops", new Integer(3));
    params.put("start_nodes", testCase.nodes.keySet().iterator().next());
    stage.setParameters(params);

    assertTrue(stage.execute());

    // Verify output is non empty.
    try {
      FileInputStream inStream = new FileInputStream(
          FilenameUtils.concat(outputPath.getPath(), "subgraph.avro"));
      ArrayList<GraphNodeData> nodes = new ArrayList<GraphNodeData>();
      SpecificDatumReader<GraphNodeData> reader =
          new SpecificDatumReader<GraphNodeData>();
      DataFileStream<GraphNodeData> avro_stream =
          new DataFileStream<GraphNodeData>(inStream, reader);
      while(avro_stream.hasNext()) {
        GraphNodeData data  = avro_stream.next();
        nodes.add(data);
      }
      assertTrue(nodes.size() > 1);

    } catch (IOException exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
