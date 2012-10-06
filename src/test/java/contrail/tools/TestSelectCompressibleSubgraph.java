package contrail.tools;

import static org.junit.Assert.fail;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.KMerReadTag;
import contrail.tools.SelectCompressibleSubgraph;
import contrail.util.FileHelper;

public class TestSelectCompressibleSubgraph {

  @Test
  public void testRun() {
    // Test currently only ensures the job runs.

    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGATT", 3);

    // Add some tips.
    builder.addEdge("ATT", "TTG", 2);
    builder.addEdge("ATT", "TTC", 2);

    KMerReadTag readTag = new KMerReadTag("compress", 0);

    builder.findNodeForSequence("ATT").setMertag(readTag);
    builder.findNodeForSequence("TTG").setMertag(readTag);

    File temp = FileHelper.createLocalTempDir();
    File avroFile = new File(temp, "graph.avro");

    GraphUtil.writeGraphToFile(
        avroFile, builder.getAllNodes().values());

    // Run it.
    SelectCompressibleSubgraph stage = new SelectCompressibleSubgraph();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());

    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString()};

    // Catch the following after debugging.
    try {
      stage.run(args);
    } catch (Exception exception) {
      exception.printStackTrace();
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
