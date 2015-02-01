package contrail.dataflow.transforms;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.codec.binary.Base64InputStream;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.values.KV;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;

public class TestGraphNodeToBase64EncodedKV {
  @Test
  public void testDoFn() throws IOException {
    DoFnTester<GraphNodeData, KV<String, String>> tester = DoFnTester.of(
        new GraphNodeToBase64EncodedKV());

    GraphNode inputData = new GraphNode();
    inputData.setNodeId("someNode");
    inputData.setSequence(new Sequence("ACTCG", DNAAlphabetFactory.create()));
    List<KV<String, String>> outputs = tester.processBatch(inputData.getData());

    KV<String, String> output = outputs.get(0);
    assertEquals("someNode", output.getKey());

    ByteArrayInputStream rawStream = new ByteArrayInputStream(
        output.getValue().getBytes());

    Base64InputStream base64Stream = new Base64InputStream(rawStream);

    SpecificDatumReader<GraphNodeData> reader =
        new SpecificDatumReader<GraphNodeData>(GraphNodeData.class);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
        base64Stream, null);

    GraphNodeData decodedData = reader.read(null, decoder);
    GraphNode decodedNode = new GraphNode(decodedData);
    assertEquals(inputData, decodedNode);
  }
}
