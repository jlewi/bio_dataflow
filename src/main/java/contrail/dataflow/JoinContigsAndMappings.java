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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.AsIterable;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import contrail.dataflow.JoinMappingsAndReads.BuildResult;
import contrail.dataflow.JoinMappingsAndReads.OutputContigAsFastaDo;
import contrail.dataflow.JoinMappingsAndReads.OutputReadAsFastqDo;
import contrail.dataflow.transforms.JoinContigsReadsMapppings;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigReadAlignment;
import contrail.sequences.FastUtil;
import contrail.sequences.FastaRecord;
import contrail.sequences.Read;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * Join the bowtie mappings, the contigs, and the reads.
 *
 * The goal is to be able to evaluate how the mappings align to the reads.
 */
// TODO(jlewi): Rename this to reflect that we are joining the contigs and
// the mappings.
public class JoinContigsAndMappings extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      JoinContigsAndMappings.class);
  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ContrailParameters.add(defs, new ParameterDefinition(
        "bowtie_alignments",
        "The GCS path to the avro files containing the alignments " +
        "produced by bowtie of the reads to the contigs.",
        String.class, null));

    ContrailParameters.add(defs, new ParameterDefinition(
        "contigs", "The glob on GCS tp the avro " +
        "files containing the GraphNodeData records representing the " +
        "graph.", String.class, null));

    ContrailParameters.add(defs, new ParameterDefinition(
        "reads", "The GCS path to the avro " +
        "files containing the reads.", String.class, null));

    for (ParameterDefinition def : ContrailParameters.getInputOutputPathOptions() ){
      if (def.getName().equals("outputpath")) {
        defs.put(def.getName(), def);
      }
    }

    ContrailParameters.add(defs, new ParameterDefinition(
            "runner", "The pipeline runner to use.",
            String.class, "DirectPipelineRunner"));

    ContrailParameters.addList(defs,  DataflowParameters.getDefinitions());
    return Collections.unmodifiableMap(defs);
  }


  @Override
  protected void stageMain() {
    String readsPath = (String) stage_options.get("reads");
    String contigsPath = (String) stage_options.get("contigs");
    String bowtieAlignmentsPath = (String) stage_options.get(
        "bowtie_alignments");
    Date now = new Date();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-hhmmss");
    // N.B. We don't use FilenameUtils.concat because it messes up the URI
    // prefix.
    String outputPath = (String) stage_options.get("outputpath");
    if (!outputPath.endsWith("/")) {
      outputPath += "/";
    }
    outputPath += formatter.format(now);
    outputPath += "/";

    PipelineOptions options = new PipelineOptions();
    DataflowParameters.setPipelineOptions(stage_options, options);

    Pipeline p = Pipeline.create();

    DataflowUtil.registerAvroCoders(p);

    PCollection<GraphNodeData> nodes = ReadAvroSpecificDoFn.readAvro(
        GraphNodeData.class, p, options, contigsPath);

    PCollection<Read> reads = ReadAvroSpecificDoFn.readAvro(
        Read.class, p, options, readsPath);
    PCollection<BowtieMapping> mappings = ReadAvroSpecificDoFn.readAvro(
        BowtieMapping.class, p, options, bowtieAlignmentsPath);

    JoinContigsReadsMapppings joinContigsReadsMappings =
        new JoinContigsReadsMapppings();

    PCollection<ContigReadAlignment> joined =
        joinContigsReadsMappings.apply(PCollectionTuple
          .of(joinContigsReadsMappings.nodeTag, nodes)
          .and(joinContigsReadsMappings.mappingTag, mappings)
          .and(joinContigsReadsMappings.readTag, reads));

    // TODO(jlewi):
    // We should convert the avro records to 1 line json and then output them.

    p.run(PipelineRunner.fromOptions(options));

    sLogger.info("Output written to: " + outputPath);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new JoinContigsAndMappings(), args);
    System.exit(res);
  }
}

