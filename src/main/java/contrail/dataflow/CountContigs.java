package contrail.dataflow;

import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

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
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;

import contrail.graph.GraphNodeData;
import contrail.scaffolding.FilterReads;
import contrail.stages.NonMRStage;

public class CountContigs extends NonMRStage {
//  static PCollectionList<String> makeSplits(
//      boolean ordered, Pipeline p, Coder<GCSAvroFileSplit> coder, List<String>... lists) {
//    List<PCollection<GCSAvroFileSplit>> inputs = new ArrayList<>();
//    for (List<GCSAvroFileSplit> list : lists) {
//      
//      if (ordered) {
//        pc.setOrdered(true);
//      }
//      pcs.add(pc);
//    }
//    return PCollectionList.of(pcs);
//  }
//  
//  /** 
//   * A PTransform to read avro files.
//   */
//  public static class ReadAvro<T> extends PTransform<PCollection<GCSAvroFileSplit>, PCollection<T>> {
//    @Override
//    public PCollection<String> apply(PCollection<String> lines) {
//      
//      // Convert lines of text into individual words.
//      PCollection<String> words = lines.apply(
//          ParDo.of(new ExtractWordsFn()));
//
//      // Count the number of times each word occurs.
//      PCollection<KV<String, Long>> wordCounts =
//          words.apply(Count.<String>create());
//
//      // Format each word and count into a printable string.
//      PCollection<String> results = wordCounts.apply(
//          ParDo.of(new FormatCountsFn()));
//      
//      PCollection<T> results = wordCounts.apply(
//          ParDo.of(new FormatCountsFn()));
//      return results;
//    }
//
//    @Override
//    public PCollection<T> apply(PCollection<GCSAvroFileSplit> input) {
//      
//      return results;
//    }
//  }
  
  /** A DoFn that turns a GCSAvroFileSplit into elements. */
  public static class ReadAvro<T> extends DoFn<GCSAvroFileSplit, String> {
    @Override
    public void processElement(ProcessContext c) {
      GCSAvroFileSplit split = (GCSAvroFileSplit) c.element();
      System.out.println(split.getPath());
      
      c.output("String");
//      String[] words = c.element().split("[^a-zA-Z']+");
//      for (String word : words) {
//        if (!word.isEmpty()) {
//          c.output(word);
//        }
//      }
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
//
//    AvroSpecificCoder<GCSAvroFileSplit> splitCoder = new AvroSpecificCoder<GCSAvroFileSplit>(new GCSAvroFileSplit().getSchema());
//        AvroSpecificCoder<GraphNodeData> nodeCoder = new AvroSpecificCoder<GraphNodeData>(new GraphNodeData().getSchema());
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
    
    inputs.apply(ParDo.of(new ReadAvro<GraphNodeData>()));

    p.run(PipelineRunner.fromOptions(options));
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CountContigs(), args);
    System.exit(res);
  }
}
