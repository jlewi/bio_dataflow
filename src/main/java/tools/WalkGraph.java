package tools;

import java.io.FileInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;

import contrail.avro.ContrailParameters;
import contrail.avro.ParameterDefinition;
import contrail.graph.GraphNodeData;

/**
 * This class is used to walk a graph starting at some seed nodes
 * and walking outwards. All nodes visited are then outputted.
 * The input files must be SortedKeyValueFile's so that we can efficiently
 * lookup nodes in the files.
 */
public class WalkGraph {
  protected Map<String, ParameterDefinition>
  createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  private SortedKeyValueFile.Reader createReader() {
    SortedKeyValueFile.Writer.Options reader_options =
        new SortedKeyValueFile.Reader.Options();

    SortedKeyValueFile.Writer<String, GraphNodeData> writer =
//      new SortedKeyValueFile.Writer<String,GraphNodeData> (writer_options);
    writer_options.withConfiguration(getConf());
    writer_options.withKeySchema(Schema.create(Schema.Type.STRING));
    writer_options.withValueSchema(node_data.getSchema());
    writer_options.withPath(new Path(output_path));
  }
  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath", "seednodes"};
    checkHasParametersOrDie(required_args);

    String input_path = (String) stage_options.get("inputpath");
    String output_path = (String) stage_options.get("outputpath");
    String seednodes = (String) stage_options.get("outputpath");
    Integer hops = (Integer) stage_options.get("hops");

    String nodeids = seednodes.split(",");

    // Keep track of nodes visited so we only output each node once.
    HashSet<String> nodes_visited = new HashSet<String>;


//    // Read the input file. We use a stream because we don't need random
//    // access to the file.
//    // TODO(jlewi): We should use hadoop classes so we can read files directly
//    // from HDFS.
//    FileInputStream in_stream = new FileInputStream(input_path);
//    SpecificDatumReader<GraphNodeData> reader =
//        new SpecificDatumReader<GraphNodeData>(GraphNodeData.class);
//
//    DataFileStream<GraphNodeData> avro_stream =
//        new DataFileStream<GraphNodeData>(in_stream, reader);
//
//    GraphNodeData node_data = new GraphNodeData();
//
//    SortedKeyValueFile.Writer.Options writer_options =
//        new SortedKeyValueFile.Writer.Options();
//
//    writer_options.withConfiguration(getConf());
//    writer_options.withKeySchema(Schema.create(Schema.Type.STRING));
//    writer_options.withValueSchema(node_data.getSchema());
//    writer_options.withPath(new Path(output_path));
//
//    SortedKeyValueFile.Writer<String, GraphNodeData> writer =
//        new SortedKeyValueFile.Writer<String,GraphNodeData> (writer_options);
//
//    Schema node_schema = node_data.getSchema();
//    while(avro_stream.hasNext()) {
//      avro_stream.next(node_data);
//
//      //writer.append(node_data.getNodeId().toString(), node_data);
//      // Key it by mertag.
//      String mertag = node_data.getMertag().getReadTag().toString() + "_" +
//          node_data.getMertag().getChunk();
//      writer.append(mertag, node_data);
//    }

    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WalkGraph(), args);
    System.exit(res);
  }
}
