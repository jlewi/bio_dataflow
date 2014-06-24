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

import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.runners.PipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollections;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import contrail.graph.GraphNodeData;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigReadAlignment;
import contrail.sequences.Read;
import contrail.stages.NonMRStage;

public class JoinMappingsAndReads extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      TestJoinMappingsAndReads.class);

  protected PCollection<GraphNodeData> readGraphNodes(Pipeline p) {
    GCSAvroFileSplit split = new GCSAvroFileSplit();
    split.setPath("gs://contrail/speciesA/contigs.2013_1215/" +
                  "ContigsAfterResolveThreads/part-00089.avro");

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);
    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));

    return inputs
      .apply(ParDo.of(new ReadAvroSpecificDoFn<GraphNodeData>(
          GraphNodeData.class)));
  }

  protected PCollection<Read> readReads(
      Pipeline p) {
    GCSAvroFileSplit split = new GCSAvroFileSplit();

    split.setPath("gs://contrail/speciesA/contigs.2013_1215/" +
        "ContigsAfterResolveThreads/som-reads.avro");

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);
    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));

    return inputs
      .apply(ParDo.of(new ReadAvroSpecificDoFn<Read>(Read.class)));
  }

  protected PCollection<BowtieMapping> readMappings(Pipeline p) {
    GCSAvroFileSplit split = new GCSAvroFileSplit();

    split.setPath("gs://contrail/speciesA/contigs.2013_1215/" +
        "ContigsAfterResolveThreads/mappings.avro");

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);
    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));

    return inputs
      .apply(ParDo.of(new ReadAvroSpecificDoFn<BowtieMapping>(
          BowtieMapping.class)));
  }

  protected static class JoinMappingReadDoFn
      extends DoFn<KV<String, CoGbkResult>, ContigReadAlignment> {
    public static final TupleTag<BowtieMapping> mappingTag = new TupleTag<>();
    public static final TupleTag<Read> readTag = new TupleTag<>();

    @Override
    public void processElement(ProcessContext c) {
      KV<String, CoGbkResult> e = c.element();
      Iterable<BowtieMapping> mappingsIter = e.getValue().getAll(mappingTag);
      Iterable<Read> readIter = e.getValue().getAll(readTag);
      for (BowtieMapping mapping : mappingsIter) {
        for (Read read : readIter) {
          ContigReadAlignment alignment = new ContigReadAlignment();
          alignment.setBowtieMapping(SpecificData.get().deepCopy(
              mapping.getSchema(), mapping));
          alignment.setRead(SpecificData.get().deepCopy(
              read.getSchema(), read));
          c.output(alignment);
        }
      }
    }
  }

  protected PCollection<ContigReadAlignment> joinMappingsAndReads(
      PCollection<BowtieMapping> mappings, PCollection<Read> reads) {
    PCollection<KV<String, BowtieMapping>> keyedMappings =
        mappings.apply(ParDo.of(new BowtieMappingTransforms.KeyByReadId()))
          .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroSpecificCoder.of(BowtieMapping.class)));

    PCollection<KV<String, Read>> keyedReads =
        reads.apply(ParDo.of(new ReadTransforms.KeyByReadIdDo())).setCoder(
            KvCoder.of(
                StringUtf8Coder.of(),
                AvroSpecificCoder.of(Read.class)));


    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
      KeyedPCollections.of(JoinMappingReadDoFn.mappingTag, keyedMappings)
                       .and(JoinMappingReadDoFn.readTag, keyedReads)
                       .apply(CoGroupByKey.<String>create());


    PCollection<ContigReadAlignment> joined =
        coGbkResultCollection.apply(ParDo.of(new JoinMappingReadDoFn()));

    return joined;
  }

  @Override
  protected void stageMain() {
    PipelineOptions options = new PipelineOptions();
    options.runner = "DirectPipelineRunner";

    Pipeline p = Pipeline.create();

    DataflowUtil.registerAvroCoders(p);

    PCollection<GraphNodeData> nodes = readGraphNodes(p);
    PCollection<Read> reads = readReads(p);
    PCollection<BowtieMapping> mappings = readMappings(p);

    PCollection<ContigReadAlignment> readMappingsPair = joinMappingsAndReads(
        mappings, reads);

    p.run(PipelineRunner.fromOptions(options));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new JoinMappingsAndReads(), args);
    System.exit(res);
  }
}
