package contrail.dataflow;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.avro.specific.SpecificData;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphTestUtil;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigReadAlignment;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;

public class TestJoinMappingsAndReads {
  Random generator = new Random();

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

  private Read randomRead(String readId) {
    Read read = new Read();
    read.setFastq(new FastQRecord());
    read.getFastq().setId(readId);
    read.getFastq().setQvalue("");
    read.getFastq().setRead(AlphabetUtil.randomString(
        generator, 5, DNAAlphabetFactory.create()));

    return read;
  }

  @Test
  public void testJoinMappingsAndReads() {
    PipelineOptions options = new PipelineOptions();

    Pipeline p = Pipeline.create();
    DataflowUtil.registerAvroCoders(p);

    Read readA = randomRead("readA");
    Read readB = randomRead("readB");

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

    assertEquals(2, finalResults.size());

    HashMap<String, ContigReadAlignment> results =
        new HashMap<String, ContigReadAlignment>();

    for (ContigReadAlignment tuple : finalResults) {
      results.put(tuple.getRead().getFastq().getId().toString(), tuple);
    }

    assertEquals(readA, results.get("readA").getRead());
    assertEquals(mappingA, results.get("readA").getBowtieMapping());
    assertEquals(readB, results.get("readB").getRead());
    assertEquals(mappingB, results.get("readB").getBowtieMapping());
  }

  @Test
  public void testJoinNodes() {
    ArrayList<GraphNodeData> nodes = new ArrayList<GraphNodeData>();
    ArrayList<ContigReadAlignment> alignments =
        new ArrayList<ContigReadAlignment>();

    HashMap<String, ContigReadAlignment> expected =
        new HashMap<String, ContigReadAlignment>();
    for (int i = 0; i < 2; ++i) {
      String contigId = String.format("contig%02d",  i);
      GraphNode node = GraphTestUtil.createNode(contigId, "ACTCG");
      nodes.add(node.getData());

      String readId = String.format("read%02d",  i);
      ContigReadAlignment alignment = new ContigReadAlignment();
      alignment.setBowtieMapping(emptyMapping());
      alignment.getBowtieMapping().setReadId(readId);
      alignment.getBowtieMapping().setContigId(contigId);
      alignment.setRead(randomRead(readId));

      alignments.add(alignment);

      ContigReadAlignment joined = SpecificData.get().deepCopy(
          alignment.getSchema(), alignment);
      joined.setGraphNode(node.clone().getData());
      expected.put(contigId, joined);
    }

    PipelineOptions options = new PipelineOptions();
    Pipeline p = Pipeline.create();
    DataflowUtil.registerAvroCoders(p);

    PCollection<ContigReadAlignment> alignmentsCollection = p.begin().apply(
        Create.of(alignments));

    PCollection<GraphNodeData> nodesCollection = p.begin().apply(Create.of(
        nodes));

    JoinMappingsAndReads stage = new JoinMappingsAndReads();

    PCollection<ContigReadAlignment> joined = stage.joinNodes(
        alignmentsCollection, nodesCollection);

    DirectPipelineRunner runner = DirectPipelineRunner.fromOptions(options);
    DirectPipelineRunner.EvaluationResults result = p.run(runner);
    List<ContigReadAlignment> finalResults = result.getPCollection(joined);

    assertEquals(2, finalResults.size());

    HashMap<String, ContigReadAlignment> results =
        new HashMap<String, ContigReadAlignment>();

    for (ContigReadAlignment tuple : finalResults) {
      results.put(tuple.getBowtieMapping().getContigId().toString(), tuple);
    }

    assertEquals(expected, results);
  }

  @Test
  public void testFullFilterPipeline() {
    // Test that mappings are correctly filtered by length.
    ArrayList<GraphNodeData> nodes = new ArrayList<GraphNodeData>();
    ArrayList<Read> reads = new ArrayList<Read>();
    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();

    int minLength = 10;

    HashMap<String, ContigReadAlignment> expected =
        new HashMap<String, ContigReadAlignment>();
    for (int i = 0; i < 10; ++i) {
      String contigId = String.format("contig%02d",  i);

      int length = minLength + 5;
      if (i < 5) {
        length = minLength - 2;
      }

      String contigSequence =
          AlphabetUtil.randomString(
              generator, length,
              DNAAlphabetFactory.create());
      GraphNode node = GraphTestUtil.createNode(contigId, contigSequence);
      nodes.add(node.getData());

      String readId = String.format("read%02d",  i);
      Read read = randomRead(readId);
      reads.add(read);
      BowtieMapping mapping = emptyMapping();
      mapping.setReadId(readId);
      mapping.setContigId(contigId);
      mappings.add(mapping);

      if (length >= minLength) {
        ContigReadAlignment alignment = new ContigReadAlignment();
        alignment.setBowtieMapping(SpecificData.get().deepCopy(
            mapping.getSchema(), mapping));
        alignment.setRead(SpecificData.get().deepCopy(
            read.getSchema(), read));
        alignment.setGraphNode(node.clone().getData());

        expected.put(contigId, alignment);
      }
    }

    PipelineOptions options = new PipelineOptions();
    Pipeline p = Pipeline.create();
    DataflowUtil.registerAvroCoders(p);

    PCollection<Read> readsCollection = p.begin().apply(Create.of(reads));

    PCollection<GraphNodeData> nodesCollection = p.begin().apply(Create.of(
        nodes));

    PCollection<BowtieMapping> mappingsCollection = p.begin().apply(Create.of(
        mappings));

    JoinMappingsAndReads stage = new JoinMappingsAndReads();
    stage.setParameter("min_length", minLength);

    PCollection<ContigReadAlignment> joined = stage.buildPipeline(
        p, nodesCollection, mappingsCollection, readsCollection);

    DirectPipelineRunner runner = DirectPipelineRunner.fromOptions(options);
    DirectPipelineRunner.EvaluationResults result = p.run(runner);
    List<ContigReadAlignment> finalResults = result.getPCollection(joined);

    assertEquals(expected.size(), finalResults.size());

    HashMap<String, ContigReadAlignment> results =
        new HashMap<String, ContigReadAlignment>();

    for (ContigReadAlignment tuple : finalResults) {
      results.put(tuple.getBowtieMapping().getContigId().toString(), tuple);
    }

    assertEquals(expected, results);
  }
}
