/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package contrail.dataflow.transforms;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

import contrail.graph.GraphNodeData;

public class GraphNodeToBase64EncodedKV extends DoFn<GraphNodeData, KV<String, String>> {
  private static final Logger sLogger = Logger.getLogger(
      GraphNodeToBase64EncodedKV.class);
  transient DatumWriter<GraphNodeData> datumWriter;
  transient ByteArrayOutputStream outStream;
  transient Base64OutputStream base64Stream;
  transient BinaryEncoder encoder;

  @Override
  public void startBundle(Context c) {
    datumWriter = new SpecificDatumWriter<GraphNodeData>(
        GraphNodeData.SCHEMA$);
    outStream = new ByteArrayOutputStream();
  }

  @Override
  public void processElement(
      DoFn<GraphNodeData, KV<String, String>>.ProcessContext c)
          throws Exception {
    String nodeId = c.element().getNodeId().toString();
    outStream.reset();

    base64Stream = new Base64OutputStream(outStream);
    encoder = EncoderFactory.get().binaryEncoder(base64Stream, null);

    try {
      datumWriter.write(c.element(), encoder);
    } catch (IOException exception) {
      sLogger.error(
          "There was a problem encoding the node. " +
              "Exception: " + exception.getMessage(), exception);
    }
    encoder.flush();
    base64Stream.flush();
    base64Stream.close();

    c.output(KV.of(nodeId, outStream.toString()));
  }
}