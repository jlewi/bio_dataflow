package contrail.graph;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Some miscellaneous utilities for working with graphs.
 */
public class GraphUtil {
  /**
   * Write a list of a graph nodes to an avro.
   * @param avroFile
   * @param nodes
   */
  public static void writeGraphToFile(
      File avroFile, Collection<GraphNode> nodes) {
    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datumWriter =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (GraphNode node: nodes) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. " +
           "Exception: " + exception.getMessage());
    }
  }
}
