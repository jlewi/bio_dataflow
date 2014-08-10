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

import org.apache.avro.specific.SpecificData;
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
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Lists;

import contrail.scaffolding.BowtieMapping;
import contrail.sequences.ReadId;
import contrail.sequences.ReadIdUtil;
import contrail.sequences.ReadIdUtil.ReadIdParser;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This stage checks the orientation of the reads.
 *
 * We look for mate pairs for which both reads aligned to the same read.
 * We then compare the orientation of the alignment of both reads.
 */
public class ValidateReadOrientation extends NonMRStage {
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

  // Look for mate pairs aligned to the same contig.
  public static class FilterMatePairs
      extends DoFn<KV<String, Iterable<BowtieMapping>>, List<BowtieMapping>> {
    @Override
    public void processElement(ProcessContext c) {
      List<BowtieMapping> mappings = new ArrayList<BowtieMapping>();
      for (BowtieMapping m : c.element().getValue()) {
        mappings.add(SpecificData.get().deepCopy(m.getSchema(), m));
      }

      if (mappings.size() != 2) {
        return;
      }

      String leftContig = mappings.get(0).getContigId().toString();
      String rightContig = mappings.get(1).getContigId().toString();

      if (!leftContig.equals(rightContig)) {
        return;
      }

      // Sort them lexicographically by read id.
      String leftRead = mappings.get(0).getReadId().toString();
      String rightRead = mappings.get(1).getReadId().toString();

      if (leftRead.compareTo(rightRead) > 0) {
        mappings = Lists.reverse(mappings);
      }
      c.output(mappings);
    }
  }

  // Build a table row describing the alignment.
  public static class BuildRow
      extends DoFn<List<BowtieMapping>, TableRow> {

    private transient ReadIdParser parser;

    @Override
    public void startBatch(Context c) throws Exception {
      parser = new ReadIdUtil.ReadParserUsingUnderscore();
    }

    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow();

      if (c.element().size() != 2) {
        sLogger.error("There should be 2 mappings for each row but " +
                      c.element().size() + " were found.");
        return;
      }

      ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();
      mappings.addAll(c.element());
      {
        // We need to order the mappings based on where they align to the contig.
        int[] left = new int[2];
        for (int i = 0; i < 2; ++i) {
          BowtieMapping m = c.element().get(i);
          left[i] = Math.min(m.getContigStart(), m.getContigEnd());
        }

        if (left[0] > left[1]) {
          Collections.reverse(mappings);
        }
      }

      String orientation = "";
      for (int i = 0; i < mappings.size(); ++i) {
        BowtieMapping m = mappings.get(i);
        String prefix = "";
        if (i == 0) {
          prefix = "left";
        } else if (i == 1) {
          prefix = "right";
        }

        String strand = "F";
        if (m.getContigStart() > m.getContigEnd()) {
          strand = "R";
        }
        orientation += strand;

        row.set(prefix + "_read_id", m.getReadId().toString());
        row.set(prefix + "_contig_id", m.getContigId().toString());
        row.set(prefix + "_contig_start", m.getContigStart());
        row.set(prefix + "_contig_end", m.getContigEnd());

        // Compute the actual left and right coordinates.
        row.set(prefix + "_contig_left",
                Math.min(m.getContigStart(), m.getContigEnd()));
        row.set(prefix + "_contig_right",
                Math.max(m.getContigStart(), m.getContigEnd()));
        row.set(prefix + "_num_mismatches", m.getNumMismatches());
      }
      BowtieMapping left = mappings.get(0);
      BowtieMapping right = mappings.get(1);
      int distance =
          Math.min(right.getContigEnd(), right.getContigStart());
          Math.max(left.getContigEnd(), left.getContigStart());

      ReadId leftId = parser.parse(left.getReadId().toString());

      // TODO(jeremy@lewi.us): Check if library for left and right read
      // matches and if not increment a counter.
      row.set("orientation", orientation);
      row.set("distance", distance);
      row.set("library", leftId.getLibrary());
      c.output(row);
    }
  }

  private PCollection<TableRow> buildPipeline(
      Pipeline p, PCollection<BowtieMapping> mappings) {
    PCollection<KV<String, BowtieMapping>> keyed = mappings.apply(
        ParDo.of(new BowtieMappingTransforms.KeyByMatedId())
            .named("KeyByMateId"))
            .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroSpecificCoder.of(BowtieMapping.class)));

    PCollection<KV<String, Iterable<BowtieMapping>>> paired =
        keyed.apply(GroupByKey.<String, BowtieMapping>create());

    PCollection<List<BowtieMapping>> mates = paired.apply(ParDo.of(
        new FilterMatePairs()).named("FilterMatePairs"));

    PCollection<TableRow> rows = mates.apply(ParDo.of(
        new BuildRow()).named("ToBQRow"));

    return rows;
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    PipelineOptions options = new PipelineOptions();
    DataflowParameters.setPipelineOptions(stage_options, options);

    Pipeline p = Pipeline.create();

    DataflowUtil.registerAvroCoders(p);

    PCollection<BowtieMapping> mappings = ReadAvroSpecificDoFn.readAvro(
        BowtieMapping.class, p, options, inputPath);

    PCollection<TableRow> rows = buildPipeline(p, mappings);

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("orientation").setType("STRING"));
    fields.add(new TableFieldSchema().setName("distance").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("library").setType("STRING"));
    for (String prefix : new String[]{"left", "right"}) {
      for (String name : new String[]{"_contig_id", "_read_id"}) {
        TableFieldSchema schema = new TableFieldSchema().setName(
            prefix + name).setType("STRING");
        fields.add(schema);
      }
      for (String name : new String[]{"_contig_start", "_contig_end",
                                      "_contig_left", "_contig_right",
                                      "_num_mismatches"}) {
        TableFieldSchema schema = new TableFieldSchema().setName(
            prefix + name).setType("INTEGER");
        fields.add(schema);
      }
    }

    TableSchema schema = new TableSchema().setFields(fields);

    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    Date date = new Date();
    String timestamp = formatter.format(date);

    String output = (String) stage_options.get("output") + "-" + timestamp;
    rows.apply(BigQueryIO.Write
        .named("Write")
        .to(output)
        .withSchema(schema)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

    p.run(PipelineRunner.fromOptions(options));
    sLogger.info("Output written to: " + output);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new ValidateReadOrientation(), args);
    System.exit(res);
  }
}
