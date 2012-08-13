package contrail.tools;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.graph.GraphNodeData;

/**
 * Write the nodes with the given MerTag to a file.
 * This is useful if we want to make a graph of what quickmerge sees.
 */
public class SelectNodesWithMerTag  extends Stage {
  protected Map<String, ParameterDefinition>
  createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition mertag = new ParameterDefinition(
        "mertag", "The directory containing the input",
        String.class,
        null);
    defs.put(mertag.getName(), mertag);
    return Collections.unmodifiableMap(defs);
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath", "mertag"};
    checkHasParametersOrDie(required_args);

    String input_path = (String) stage_options.get("inputpath");
    String output_path = (String) stage_options.get("outputpath");
    String target_mertag = (String) stage_options.get("mertag");

    // Read the input file. We use a stream because we don't need random
    // access to the file.
    // TODO(jlewi): We should use hadoop classes so we can read files directly
    // from HDFS.
    FileInputStream in_stream = new FileInputStream(input_path);
    SpecificDatumReader<GraphNodeData> reader =
        new SpecificDatumReader<GraphNodeData>(GraphNodeData.class);

    DataFileStream<GraphNodeData> avro_stream =
        new DataFileStream<GraphNodeData>(in_stream, reader);

    GraphNodeData node_data = new GraphNodeData();


    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datum_writer =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datum_writer);

    try {
      writer.create(schema, new File(output_path));
      //      for (MapperInputOutput input_output: test_case.inputs_outputs.values()) {
      //        writer.append(input_output.input_node);
      //      }
      //      writer.close();

      while(avro_stream.hasNext()) {
        avro_stream.next(node_data);

        String mertag = node_data.getMertag().getReadTag().toString() + "_" +
            node_data.getMertag().getChunk();
        if (mertag.equals(target_mertag)) {
          writer.append(node_data);
        }
        // Key it by mertag.
        //      String mertag = node_data.getMertag().getReadTag().toString() + "_" +
        //          node_data.getMertag().getChunk();
        //      writer.append(mertag, node_data);
      }

    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }
    finally {
      writer.close();
    }

    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new SelectNodesWithMerTag(), args);
    System.exit(res);
  }
}
