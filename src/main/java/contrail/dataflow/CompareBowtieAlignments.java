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
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import contrail.scaffolding.BowtieMapping;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

public class CompareBowtieAlignments extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      CompareBowtieAlignments.class);

  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ContrailParameters.add(defs, new ParameterDefinition(
        "full_alignments",
        "The GCS path to a text file containing the alignments " +
            "of the full reads to a subset of contigs.",
            String.class, null));

    ContrailParameters.add(defs, new ParameterDefinition(
        "short_alignments", "The glob on GCS to the avro " +
            "files containing the BowtieAlignment records representing the " +
            "alignments of the shortened reads.", String.class, null));

    ContrailParameters.add(defs, new ParameterDefinition(
        "output", "The table to write to. This should be in the form " +
            "<project>:<dataset>.<table> table is overwritten if it exists.",
            String.class, null));

    ContrailParameters.addList(defs,  DataflowParameters.getDefinitions());
    return Collections.unmodifiableMap(defs);
  }

  protected static class JoinMappingsDoFn
  extends DoFn<KV<String, CoGbkResult>, TableRow> {
    private final TupleTag<BowtieMapping> shortTag;
    private final TupleTag<BowtieMapping> fullTag;

    public JoinMappingsDoFn(
        TupleTag<BowtieMapping> fullTag, TupleTag<BowtieMapping> shortTag) {
      this.fullTag = fullTag;
      this.shortTag = shortTag;
    }

    @Override
    public void processElement(ProcessContext c) {
      KV<String, CoGbkResult> e = c.element();
      Iterable<BowtieMapping> fullIter = e.getValue().getAll(fullTag);
      Iterable<BowtieMapping> shortIter = e.getValue().getAll(shortTag);
      for (BowtieMapping fullMapping : fullIter) {
        String fullStrand = "F";
        if (fullMapping.getContigStart() > fullMapping.getContigEnd()) {
          fullStrand = "R";
        }

        for (BowtieMapping shortMapping : shortIter) {
          String shortStrand = "F";
          if (shortMapping.getContigStart() > shortMapping.getContigEnd()) {
            shortStrand = "R";
          }

          TableRow row = new TableRow();
          row.set("read_id", e.getKey());
          row.set("full_strand", fullStrand);
          row.set("short_strand", shortStrand);
          c.output(row);
        }
      }
    }
  }

  protected PCollection<TableRow> buildPipeline(
      Pipeline p,
      PCollection<BowtieMapping> fullAlignments,
      PCollection<BowtieMapping> shortAlignments) {
    PCollection<KV<String, BowtieMapping>> keyedFull = fullAlignments.apply(
        ParDo.of(new BowtieMappingTransforms.KeyByReadId()).named("KeyFull"))
          .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroSpecificCoder.of(BowtieMapping.class)));
    PCollection<KV<String, BowtieMapping>> keyedShort = shortAlignments.apply(
        ParDo.of(new BowtieMappingTransforms.KeyByReadId()).named("KeyShort"))
          .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroSpecificCoder.of(BowtieMapping.class)));

    final TupleTag<BowtieMapping> fullTag = new TupleTag<>();
    final TupleTag<BowtieMapping> shortTag = new TupleTag<>();
    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
        KeyedPCollectionTuple.of(fullTag, keyedFull)
        .and(shortTag, keyedShort)
        .apply(CoGroupByKey.<String>create());


    // Get the ids of the contigs we are keeping.
    PCollection<TableRow> rows = coGbkResultCollection.apply(
        ParDo.of(new JoinMappingsDoFn(fullTag, shortTag)).named(
            "JoinMappings"));

    return rows;
  }

  @Override
  protected void stageMain() {
    String fullAlignmentsPath = (String) stage_options.get("full_alignments");
    String shortAlignmentsPath = (String) stage_options.get("short_alignments");

    PipelineOptions options = new PipelineOptions();
    DataflowParameters.setPipelineOptions(stage_options, options);

    Pipeline p = Pipeline.create();

    DataflowUtil.registerAvroCoders(p);

    PCollection<String> lines = p.begin().apply(
        TextIO.Read.named("ReadFullAlignments")
        .from(fullAlignmentsPath));

    PCollection<BowtieMapping> fullAlignments = lines.apply(ParDo.of(
        new BowtieMappingTransforms.ParseMappingLineDo()).named(
            "ParseMappingLineDo"));

    PCollection<BowtieMapping> shortAlignments = ReadAvroSpecificDoFn.readAvro(
        BowtieMapping.class, p, options, shortAlignmentsPath);

    PCollection<TableRow> rows = buildPipeline(
        p, fullAlignments, shortAlignments);

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("read_id").setType("STRING"));
    fields.add(new TableFieldSchema().setName("full_strand").setType("STRING"));
    fields.add(new TableFieldSchema().setName("short_strand").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);

    rows.apply(BigQueryIO.Write
        .named("Write")
        .to((String) stage_options.get("output"))
        .withSchema(schema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run(PipelineRunner.fromOptions(options));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CompareBowtieAlignments(), args);
    System.exit(res);
  }
}