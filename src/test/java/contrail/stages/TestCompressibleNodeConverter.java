package contrail.stages;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.util.FileHelper;

public class TestCompressibleNodeConverter {
  private List<CompressibleNodeData> createTestInputs() {
    List<CompressibleNodeData> inputs = new ArrayList<CompressibleNodeData>();
    for (int i = 0; i < 10; ++i) {
      GraphNode node = new GraphNode();
      node.setNodeId("Node:" + i);
      node.setSequence(new Sequence("ACTGA", DNAAlphabetFactory.create()));
      CompressibleNodeData compressibleNode = new CompressibleNodeData();
      compressibleNode.setNode(node.getData());
      compressibleNode.setCompressibleStrands(CompressibleStrands.NONE);
      inputs.add(compressibleNode);
    }
    return inputs;
  }

  private File createInputFile(
      File tempDir, List<CompressibleNodeData> inputs) {
    File avroFile = new File(tempDir, "compressed.avro");

    // Write the data to the file.
    Schema schema = (new CompressibleNodeData()).getSchema();
    DatumWriter<CompressibleNodeData> datumWriter =
        new SpecificDatumWriter<CompressibleNodeData>(schema);
    DataFileWriter<CompressibleNodeData> writer =
        new DataFileWriter<CompressibleNodeData>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (CompressibleNodeData bubble : inputs) {
        writer.append(bubble);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the nodes to an avro file. Exception:" +
           exception.getMessage());
    }

    return avroFile;
  }

  @Test
  public void testRun() {
    List<CompressibleNodeData> inputs = createTestInputs();
    File tempDir = FileHelper.createLocalTempDir();
    createInputFile(tempDir, inputs);
    // Run it.
    CompressibleNodeConverter stage = new CompressibleNodeConverter();
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
