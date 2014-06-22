package contrail.dataflow;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.ByteChannel;
import java.nio.channels.Channels;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.GcsUtil.GcsFilename;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import contrail.graph.GraphNodeData;
import contrail.scaffolding.FilterReads;
import contrail.stages.BuildGraphAvro;
import contrail.stages.NonMRStage;
import contrail.util.AvroSchemaUtil;

public class CountContigs extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(CountContigs.class);  
  /** A DoFn that turns a GCSAvroFileSplit into elements. */
  public static class ReadAvro extends DoFn<GCSAvroFileSplit,GraphNodeData> {
    private Class specificTypeClass;
    
    public ReadAvro(Class specificTypeClass) {
      this.specificTypeClass = specificTypeClass;
    }
    
    @Override
    public void processElement(ProcessContext c) {
      GCSAvroFileSplit split = (GCSAvroFileSplit) c.element();
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
  
  public static class GCSAvroFileSplitCoder extends AvroSpecificCoder<GCSAvroFileSplit> {    
    public GCSAvroFileSplitCoder() {
      super(new GCSAvroFileSplit().getSchema());
    }
    public static GCSAvroFileSplitCoder of(){    
      return new GCSAvroFileSplitCoder();
    } 
  }
  
  public static class GraphNodeDataCoder  extends AvroSpecificCoder<GraphNodeData> {
    public GraphNodeDataCoder() {      
      super(new GraphNodeData().getSchema());
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

    // Register default coders.
    p.getCoderRegistry().registerCoder(
        GCSAvroFileSplit.class, 
        GCSAvroFileSplitCoder.class);
    p.getCoderRegistry().registerCoder(
        GraphNodeData.class, 
        GraphNodeDataCoder.class);
    
    GCSAvroFileSplit split = new GCSAvroFileSplit();
    split.setPath("gs://contrail/speciesA/contigs.2013_1215/" +
                  "ContigsAfterResolveThreads/part-00089.avro");

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);
    
    // TODO(jeremy@lewi.us): Is AvroCoder using the fact that GCSAvroFileSplit
    // is a specific Avro type? Do we need to write our own custom coder?
    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));
    
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
