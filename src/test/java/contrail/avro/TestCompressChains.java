//Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.avro;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;

//import java.io.File;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
//import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.Reporter;
//import org.junit.Test;
//
//import contrail.ReporterMock;
//import contrail.graph.EdgeTerminal;
//import contrail.graph.GraphNode;
//import contrail.graph.SimpleGraphBuilder;
//import contrail.sequences.DNAAlphabetFactory;
//import contrail.sequences.DNAStrand;
//import contrail.sequences.Sequence;


public class TestCompressChains extends CompressChains {
  @Test
  public void testRun() {
    // Create a graph and write it to file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGATT", 3);
    
    File temp = null;

    try {
      temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
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
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datum_writer =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datum_writer);

    try {
      writer.create(schema, avro_file);
      for (GraphNode node: builder.getAllNodes().values()) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }

    // Run it.
    CompressChains compress_chains = new CompressChains();
    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
       "--K=3"};

    try {
      compress_chains.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
