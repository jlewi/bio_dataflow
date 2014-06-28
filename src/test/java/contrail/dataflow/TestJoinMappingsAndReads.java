package contrail.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.IterableCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.join.RawUnionValue;
import com.google.cloud.dataflow.sdk.transforms.join.UnionCoder;
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

  @Test
  public void testUnionCoder() throws CoderException, IOException {
    // The purpose of this test is to try to reproduce the serialization errors
    // we are getting.
    List<Coder> coders = new ArrayList<Coder>();
    coders.add(AvroSpecificCoder.of(BowtieMapping.class));
    coders.add(AvroSpecificCoder.of(Read.class));
    UnionCoder unionCoder = UnionCoder.of(coders);

    IterableCoder<RawUnionValue> icoder = IterableCoder.of(unionCoder);

    BowtieMapping mapping = emptyMapping();
    mapping.setReadId("readA");
    Context c;

    Read readA = new Read();
    readA.setFastq(new FastQRecord());
    readA.getFastq().setId("readA");
    readA.getFastq().setQvalue("");
    readA.getFastq().setRead("ACTCG");

    ArrayList<RawUnionValue> values = new ArrayList<RawUnionValue>();
    RawUnionValue value1 = new RawUnionValue(0, mapping);
    RawUnionValue value2 = new RawUnionValue(1, readA);
    values.add(value1);
    values.add(value2);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    // TODO(jlewi): Try with true/false for whole stream.
    Context context = new Context(false);
    icoder.encode(values, outStream, context);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    icoder.decode(inStream, context);
  }

  @Test
  public void testUnionCoderStrings() throws CoderException, IOException {
    // The purpose of this test is to try to reproduce the serialization errors
    // we are getting but using simple coders rather than an avro schema.
    List<Coder> coders = new ArrayList<Coder>();
    coders.add(StringUtf8Coder.of());
    coders.add(VarIntCoder.of());
    UnionCoder unionCoder = UnionCoder.of(coders);

    IterableCoder<RawUnionValue> icoder = IterableCoder.of(unionCoder);

    ArrayList<RawUnionValue> values = new ArrayList<RawUnionValue>();
    RawUnionValue value1 = new RawUnionValue(0, "hello");
    RawUnionValue value2 = new RawUnionValue(1, new Integer(4));
    values.add(value1);
    values.add(value2);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    // TODO(jlewi): Try with true/false for whole stream.
    Context context = new Context(false);
    icoder.encode(values, outStream, context);

    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    icoder.decode(inStream, context);
  }


  @Test
  public void testUnionCoderNoUnion() throws CoderException, IOException {
    // The purpose of this test is to try to reproduce the serialization errors
    // we are getting but without using a schema which includes a union of a
    // a null and non null schema.
    List<Coder> coders = new ArrayList<Coder>();
    coders.add(AvroSpecificCoder.of(BowtieMapping.class));
    coders.add(AvroSpecificCoder.of(FastQRecord.class));
    UnionCoder unionCoder = UnionCoder.of(coders);

    IterableCoder<RawUnionValue> icoder = IterableCoder.of(unionCoder);

    BowtieMapping mapping = emptyMapping();
    mapping.setReadId("readA");

    FastQRecord fast = new FastQRecord();
    fast.setId("readA");
    fast.setQvalue("");
    fast.setRead("ACTCG");

    ArrayList<RawUnionValue> values = new ArrayList<RawUnionValue>();
    RawUnionValue value1 = new RawUnionValue(0, mapping);
    RawUnionValue value2 = new RawUnionValue(1, fast);
    values.add(value1);
    values.add(value2);

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    // TODO(jlewi): Try with true/false for whole stream.
    Context context = new Context(false);
    icoder.encode(values, outStream, context);

    System.out.println("Encoded bytes:\n" + outStream.toByteArray());
    ByteArrayInputStream inStream = new ByteArrayInputStream(outStream.toByteArray());
    icoder.decode(inStream, context);
  }
}
