package contrail.dataflow;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigReadAlignment;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;

public class TestJoinMappingsAndReads {

  private BowtieMapping emptyMapping() {
    BowtieMapping mapping = new BowtieMapping();
    mapping.setContigId("");
    mapping.setContigStart(0);
    mapping.setContigEnd(0);
    mapping.setNumMismatches(0);
    mapping.setRead("");
    mapping.setReadClearEnd(0);
    mapping.setReadClearStart(0);
    mapping.setReadId("");

    return mapping;
  }

  @Test
  public void testJoinMappingsAndReads() {
//    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "ACTCG");
//    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "ACTCG");

    PipelineOptions options = new PipelineOptions();

    Pipeline p = Pipeline.create();
    DataflowUtil.registerAvroCoders(p);

    Read readA = new Read();
    readA.setFastq(new FastQRecord());
    readA.getFastq().setId("readA");
    readA.getFastq().setQvalue("");
    readA.getFastq().setRead("ACTCG");

    Read readB = new Read();
    readB.setFastq(new FastQRecord());
    readB.getFastq().setId("readB");
    readB.getFastq().setQvalue("");
    readB.getFastq().setRead("ACTCG");

    PCollection<Read> reads = p.begin().apply(Create.of(
        Arrays.asList(readA, readB)));

    BowtieMapping mappingA = emptyMapping();
    mappingA.setReadId("readA");

    BowtieMapping mappingB = emptyMapping();
    mappingB.setReadId("readB");

    PCollection<BowtieMapping> mappings = p.begin().apply(Create.of(
        Arrays.asList(mappingA, mappingB)));

    JoinMappingsAndReads stage = new JoinMappingsAndReads();

    PCollection<ContigReadAlignment> joined = stage.joinMappingsAndReads(
        mappings, reads);

    DirectPipelineRunner runner = DirectPipelineRunner.fromOptions(options);
    DirectPipelineRunner.EvaluationResults result = p.run(runner);
    List<ContigReadAlignment> finalResults = result.getPCollection(joined);
    System.out.println("Number of elements:" + finalResults.size());
  }

}
