package contrail.dataflow;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsFilename;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.graph.GraphNodeData;
import contrail.stages.NonMRStage;
import contrail.util.AvroSchemaUtil;

public class CountContigs extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(CountContigs.class);
  /** A DoFn that turns a GCSAvroFileSplit into elements. */
  public static class ReadAvro extends DoFn<GCSAvroFileSplit,GraphNodeData> {
    private final Class specificTypeClass;

    public ReadAvro(Class specificTypeClass) {
      this.specificTypeClass = specificTypeClass;
    }

    @Override
    public void processElement(ProcessContext c) {
      GCSAvroFileSplit split = c.element();
      System.out.println(split.getPath());

      GcsFilename gcsFilename = GcsUtil.asGcsFilename(split.getPath().toString());

      ByteChannel inChannel;
      try {
        GcsUtil gcsUtil = GcsUtil.create(c.getPipelineOptions());
        inChannel = gcsUtil.open(gcsFilename);
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
        return;
      }
      InputStream inStream = Channels.newInputStream(inChannel);
      Schema schema = AvroSchemaUtil.getSchemaForSpecificType(
          specificTypeClass);
      SpecificDatumReader<GraphNodeData> datumReader = new SpecificDatumReader<GraphNodeData>(schema);

      DataFileStream<GraphNodeData> fileReader;
      try {
        fileReader = new DataFileStream<GraphNodeData>(inStream, datumReader);
      } catch(IOException e) {
        sLogger.error("Could not read file:" + split.getPath().toString());
        return;
      }

      while(fileReader.hasNext()) {
        GraphNodeData datum = fileReader.next();
        sLogger.info("read node");
        c.output(datum);
      }
      try {
        fileReader.close();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public static class GraphNodeDataCoder  extends AvroSpecificCoder<GraphNodeData> {
    public GraphNodeDataCoder() {
      super(GraphNodeData.class);
    }

    public static GraphNodeDataCoder of(){
      return new GraphNodeDataCoder();
    }
  }

  @Override
  protected void stageMain() {
    PipelineOptions options = new PipelineOptions();
    options.runner = "DirectPipelineRunner";

    Pipeline p = Pipeline.create();

    // Register a default coder for GraphNodeData.
    p.getCoderRegistry().registerCoder(
        GraphNodeData.class,
        GraphNodeDataCoder.class);

    GCSAvroFileSplit split = new GCSAvroFileSplit();
    split.setPath("gs://contrail/speciesA/contigs.2013_1215/" +
                  "ContigsAfterResolveThreads/part-00089.avro");

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);

    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));
    inputs.setCoder(AvroSpecificCoder.of(GCSAvroFileSplit.class));
    inputs
      .apply(ParDo.of(new ReadAvro(GraphNodeData.class)))
      .apply(ParDo.of(new GraphNodeDoFns.KeyByNodeId()));

    p.run(PipelineRunner.fromOptions(options));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CountContigs(), args);
    System.exit(res);
  }
}
