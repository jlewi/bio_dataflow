package contrail.tools;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.util.FileHelper;

public class TestCreateGraphIndex {

  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    Random generator = new Random();
    String sequence =
        AlphabetUtil.randomString(generator, 66, DNAAlphabetFactory.create());
    builder.addKMersForString(sequence, 33);

    // We split the nodes into 2 files.
    ArrayList<GraphNode> nodesLeft = new ArrayList<GraphNode>();
    ArrayList<GraphNode> nodesRight = new ArrayList<GraphNode>();

    for (GraphNode node : builder.getAllNodes().values()) {
      if (nodesLeft.size() < (builder.getAllNodes().size() / 2)) {
        nodesLeft.add(node);
      } else {
        nodesRight.add(node);
      }
    }

    // Sort the nodes by key.
    Collections.sort(nodesLeft, new GraphUtil.nodeIdComparator());
    Collections.sort(nodesRight, new GraphUtil.nodeIdComparator());

    File temp = FileHelper.createLocalTempDir();
    String inputDir = FilenameUtils.concat(temp.getPath(), "input");
    File inputDirFile = new File(inputDir);
    inputDirFile.mkdirs();
    File avroLeftFile = new File(inputDir, "graph1.avro");
    File avroRightFile = new File(inputDir, "graph2.avro");

    GraphUtil.writeGraphToFile(avroLeftFile, nodesLeft);
    GraphUtil.writeGraphToFile(avroRightFile, nodesRight);

    // Run it.
    CreateGraphIndex stage = new CreateGraphIndex();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", inputDir);

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
