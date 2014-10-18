/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package contrail.dataflow;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
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
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Keys;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.scaffolding.BowtieMapping;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * Count how many contigs each read aligns to.
 *
 * Writing the raw bowtie mappings to BigQuery and then running a query to
 * count how many distinct reads there were didn't work because distinct is
 * a statistical estimate.
 */
public class CountReadToContigAlignments extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      JoinMappingsAndReads.class);

  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def : ContrailParameters.getInputOutputPathOptions() ){
      if (def.getName().equals("inputpath")) {
        defs.put(def.getName(), def);
      }
    }

    ContrailParameters.add(defs, new ParameterDefinition(
        "output", "The table to write to. This should be in the form " +
            "<project>:<dataset>.<table> table is overwritten if it exists.",
            String.class, null));

    ContrailParameters.addList(defs,  DataflowParameters.getDefinitions());
    return Collections.unmodifiableMap(defs);
  }

  // Build a table row describing the number of alignments.
  public static class BuildRow
      extends DoFn<KV<String, Long>, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow();

      row.set("read_id", c.element().getKey());
      row.set("num_alignments", c.element().getValue());
      c.output(row);
    }
  }

  private PCollection<TableRow> buildPipeline(
      Pipeline p, PCollection<BowtieMapping> mappings) {
    PCollection<KV<String, BowtieMapping>> keyed = mappings.apply(
        ParDo.of(new BowtieMappingTransforms.KeyByReadId())
            .named("KeyByReadId"))
            .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroSpecificCoder.of(BowtieMapping.class)));

    PCollection<String> readIds = keyed.apply(Keys.<String>create());
    PCollection<KV<String, Long>> counts =
        readIds.apply(Count.<String>create());

    PCollection<TableRow> rows = counts.apply(ParDo.of(
        new BuildRow()).named("ToBQRow"));

    return rows;
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    PipelineOptions options = new PipelineOptions();
    DataflowParameters.setPipelineOptions(stage_options, options);

    Pipeline p = Pipeline.create(options);

    DataflowUtil.registerAvroCoders(p);

    PCollection<BowtieMapping> mappings = ReadAvroSpecificDoFn.readAvro(
        BowtieMapping.class, p, options, inputPath);

    PCollection<TableRow> rows = buildPipeline(p, mappings);

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("read_id").setType("STRING"));
    fields.add(new TableFieldSchema().setName("num_alignments").setType(
        "INTEGER"));
    TableSchema schema = new TableSchema().setFields(fields);

    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd_HHmmss");
    Date date = new Date();
    String timestamp = formatter.format(date);

    String output = (String) stage_options.get("output") + "_" + timestamp;
    rows.apply(BigQueryIO.Write
        .named("Write")
        .to(output)
        .withSchema(schema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run();
    sLogger.info("Output written to: " + output);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CountReadToContigAlignments(), args);
    System.exit(res);
  }
}
