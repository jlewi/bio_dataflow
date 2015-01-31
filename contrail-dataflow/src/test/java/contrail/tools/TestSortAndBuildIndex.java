package contrail.tools;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.FileHelper;


public class TestSortAndBuildIndex {
  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("TACTGGATT", 3);

    File temp = FileHelper.createLocalTempDir();
    File avroFile = new File(temp, "graph.avro");

    GraphUtil.writeGraphToFile(avroFile, builder.getAllNodes().values());

    // Run it.
    SortAndBuildIndex stage = new SortAndBuildIndex();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", avroFile.toString());

    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());

    stage.setParameters(params);

    // Catch the following after debugging.
    try {
      stage.execute();
    } catch (Exception exception) {
      exception.printStackTrace();
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
