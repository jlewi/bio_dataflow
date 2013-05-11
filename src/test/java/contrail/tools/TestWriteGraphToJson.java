package contrail.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAStrand;
import contrail.util.FileHelper;

public class TestWriteGraphToJson {
  @Test
  public void testWrite() {
    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "ACTG");
    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "CTGTT");
    nodeA.addOutgoingEdgeWithTags(
        DNAStrand.FORWARD, new EdgeTerminal("nodeB", DNAStrand.REVERSE), 
       Arrays.asList("thread1", "thread2"), 10);
    
    File tempDir = FileHelper.createLocalTempDir();
    Path avroPath = new Path(
        FilenameUtils.concat(tempDir.getPath(), "graph.avro"));
    GraphUtil.writeGraphToFile(new File(avroPath.toString()), Arrays.asList(nodeA, nodeB));

    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("inputpath", avroPath.toString());
    String outPath = FilenameUtils.concat(tempDir.getPath(), "json");
    parameters.put("outputpath", outPath);

    WriteGraphToJson stage = new WriteGraphToJson();
    stage.setParameters(parameters);
    stage.setConf(new JobConf());

    assertTrue(stage.execute());

    String jsonFile = FilenameUtils.concat(outPath, "part-00000");
    System.out.println("Output:" + jsonFile);
    try {
      FileReader reader = new FileReader(jsonFile);
      BufferedReader buffered = new BufferedReader(reader);

      String line = buffered.readLine();
      String expected =
          "{\"majorId\":\"tail\",\"minorId\":\"head\",\"paths\":[" +
          "{\"majorStrand\":\"REVERSE\",\"minorStrand\":\"REVERSE\",\"pairs\":[{\"major\":{\"id\":\"mid2\",\"strand\":\"REVERSE\",\"length\":7,\"coverage\":7.0},\"minor\":{\"id\":\"mid1\",\"strand\":\"REVERSE\",\"length\":7,\"coverage\":5.0},\"editDistance\":1,\"editRate\":0.14285715}]}]}";
    } catch(FileNotFoundException e) {
      fail(e.getMessage());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
