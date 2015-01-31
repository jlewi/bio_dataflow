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
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.dataflow.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.Configuration;

import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.dataflow.AvroMRTransforms;
import contrail.dataflow.CharSequenceCoder;
import contrail.dataflow.DataflowUtil;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.ValidateGraph;

/**
 * A dataflow for checking that a graph is valid.
 *
 * This version attempts to use as a GenericRecord as the input schema
 * so that we can handle different input types; e.g. the output of the
 * compressible stage.
 *
 * Every node sends a message to its neighbor containing:
 *   1) The id of the source node.
 *   2) The last K-1 bases of the source node.
 *   3) The strands for the edge.
 *
 * The reducer checks the following
 *   1) The destination node exists.
 *   2) The edge is valid i.e they overlap.
 *   3) The destination node has an incoming edge corresponding to that
 *      edge.
 */
public class ValidateGraphDataflow extends DataflowStage {
  private static final Logger sLogger = Logger.getLogger(
      ValidateGraphDataflow.class);
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
      HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);

    return Collections.unmodifiableMap(defs);
  }

  @Override
  public Pipeline buildPipeline(PipelineOptions options) {
    String inputPath = (String) stage_options.get("inputpath");
//    Date now = new Date();
//    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-hhmmss");

    // N.B. We don't use FilenameUtils.concat because it messes up the URI
    // prefix.
    String outputPath = (String) stage_options.get("outputpath");
//    if (!outputPath.endsWith("/")) {
//      outputPath += "/";
//    }
//    outputPath += formatter.format(now);
//    outputPath += "/";

    Pipeline p = Pipeline.create(options);

    DataflowUtil.registerAvroCoders(p);

    // TODO(jeremy@lewi.us): We should generalize this so we can handle
    // inputs which are also CompressibleNodeData.
    // To do that we would need to read a union schema and then convert
    // the GenericRecords to specific records.
    PCollection<GraphNodeData> graph = p.apply(
        AvroIO.Read.from(inputPath).withSchema(GraphNodeData.class));

    Schema stringSchema = Schema.create(Schema.Type.STRING);

    JobConf jobConf = (JobConf) getConf();
    AvroMRTransforms.AvroMapperDoFn<
        ValidateGraph.ValidateGraphMapper, Object, CharSequence, ValidateMessage>
          mapper = new AvroMRTransforms.AvroMapperDoFn<
              ValidateGraph.ValidateGraphMapper, Object, CharSequence, ValidateMessage>(
                  ValidateGraph.ValidateGraphMapper.class, jobConf,
                  stringSchema, ValidateMessage.SCHEMA$);

    KvCoder<CharSequence, ValidateMessage> mapperCoder =
      KvCoder.of(CharSequenceCoder.of(), AvroCoder.of(ValidateMessage.class));

    PCollection<KV<CharSequence, Iterable<ValidateMessage>>> pairs =
        graph.apply(ParDo.of(mapper)).setCoder(mapperCoder).apply(
            GroupByKey.<CharSequence, ValidateMessage>create());

    AvroMRTransforms.AvroReducerDoFn<
        ValidateGraph.ValidateGraphReducer, CharSequence, ValidateMessage,
        GraphError> reducer = new  AvroMRTransforms.AvroReducerDoFn<
            ValidateGraph.ValidateGraphReducer, CharSequence, ValidateMessage,
            GraphError>(ValidateGraph.ValidateGraphReducer.class, jobConf,
                        GraphError.SCHEMA$);

    PCollection<GraphError> errors = pairs.apply(ParDo.of(reducer))
        .setCoder(AvroCoder.of(GraphError.class));

    errors.apply(AvroIO.Write.to(outputPath).withSchema(GraphError.class));

    return p;
  }


  public static void main(String[] args) throws Exception {
    // TODO(jeremy@lewi.us). Do we need to use tool runner.
    int res = ToolRunner.run(
        new Configuration(), new ValidateGraphDataflow(), args);
    System.exit(res);
  }
}
