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
package contrail.dataflow.tools;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.dataflow.DataflowUtil;
import contrail.dataflow.stages.DataflowStage;
import contrail.dataflow.transforms.GraphNodeToBase64EncodedKV;
import contrail.graph.GraphNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;

/**
 * Create a table in BigQuery containing all the nodes of the graph.
 *
 * The table schema is key value pairs. The keys are the node ids and the
 * values are the base64 encoded representation of the graph nodes.
 */
public class WriteGraphToBigQuery extends DataflowStage {
  private static final Logger sLogger = Logger.getLogger(
      WriteGraphToBigQuery.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      if (def.getName().equals("inputpath")) {
        defs.put(def.getName(), def);
      }
    }

    ParameterDefinition outputTable = new ParameterDefinition(
        "output_table",
        "The output table to write to. This should be in the form " +
            "dataset_id.table_id", String.class,
            null);
    ContrailParameters.add(defs,  outputTable);
    return Collections.unmodifiableMap(defs);
  }

  public static class ConvertToBQRow
  extends DoFn<KV<String, String>, TableRow> {
    @Override
    public void processElement(
        DoFn<KV<String, String>, TableRow>.ProcessContext c)
            throws Exception {
      TableRow row = new TableRow();
      row.set("node_id", c.element().getKey());
      row.set("node_data", c.element().getValue());
      c.output(row);
    }
  }

  @Override
  protected Pipeline buildPipeline(PipelineOptions options) {
    Pipeline p = Pipeline.create(options);

    DataflowUtil.registerAvroCoders(p);
    PCollection<GraphNodeData> nodes = p.apply(
        AvroIO.Read.from((String)stage_options.get("inputpath"))
        .withSchema(GraphNodeData.class));

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema()
       .setName("node_id")
       .setType("STRING"));
    fields.add(new TableFieldSchema()
       .setName("node_data")
       .setType("STRING"));
    TableSchema tableSchema = new TableSchema().setFields(fields);
    nodes.apply(ParDo.of(new GraphNodeToBase64EncodedKV()))
      .apply(ParDo.of(new ConvertToBQRow()))
      .apply(BigQueryIO.Write
          .to((String)stage_options.get("output_table"))
          .withSchema(tableSchema));

    return p;
  }

  public static void main(String[] args) throws Exception {
    // TODO(jeremy@lewi.us). Do we need to use tool runner.
    int res = ToolRunner.run(
        new Configuration(), new WriteGraphToBigQuery(), args);
    System.exit(res);
  }
}
