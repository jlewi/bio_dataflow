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
package contrail.dataflow.transforms;

import org.apache.avro.specific.SpecificData;

import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.DoFn.ProcessContext;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import contrail.dataflow.AvroSpecificCoder;
import contrail.dataflow.BowtieMappingTransforms;
import contrail.dataflow.GraphNodeTransforms;
import contrail.dataflow.JoinMappingsAndReads.JoinMappingReadDoFn;
import contrail.dataflow.JoinMappingsAndReads.JoinNodesDoFn;
import contrail.dataflow.JoinMappingsAndReads.KeyByContigId;
import contrail.dataflow.ReadTransforms;
import contrail.graph.GraphNodeData;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigReadAlignment;
import contrail.sequences.Read;

/**
 * Transforms for joining the contigs, bowtie alignments, and reads.
 */
public class JoinContigsReadsMapppings extends PTransform<PCollectionTuple, PCollection<ContigReadAlignment>> {
  final public TupleTag<GraphNodeData> nodeTag = new TupleTag<>();
  final public TupleTag<BowtieMapping> mappingTag = new TupleTag<>();
  final public TupleTag<Read> readTag = new TupleTag<>();
  final public TupleTag<ContigReadAlignment> alignmentTag = new TupleTag<>();

  /**
   * A transform for joining the bowtie mappings with the reads.
   */
  private static class JoinMappingsAndReadsTransform extends
      PTransform<PCollectionTuple, PCollection<ContigReadAlignment>> {
    final public TupleTag<BowtieMapping> mappingTag;
    final public TupleTag<Read> readTag;

    private class JoinMappingReadDoFn extends DoFn<KV<String, CoGbkResult>, ContigReadAlignment> {
      private final TupleTag<BowtieMapping> mappingTag;
      private final TupleTag<Read> readTag;

      public JoinMappingReadDoFn(
          TupleTag<BowtieMapping> mappingTag, TupleTag<Read> readTag) {
        this.mappingTag = mappingTag;
        this.readTag = readTag;
      }

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

    public JoinMappingsAndReadsTransform(
        TupleTag<BowtieMapping> mappingTag,
        TupleTag<Read> readTag) {
      this.mappingTag = mappingTag;
      this.readTag = readTag;
    }

    @Override
    public PCollection<ContigReadAlignment> apply(PCollectionTuple inputTuple) {
      PCollection<BowtieMapping> mappings = inputTuple.get(mappingTag);
      PCollection<Read> reads = inputTuple.get(readTag);
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
          KeyedPCollectionTuple.of(mappingTag, keyedMappings)
          .and(readTag, keyedReads)
          .apply(CoGroupByKey.<String>create());


      PCollection<ContigReadAlignment> joined =
          coGbkResultCollection.apply(ParDo.of(new JoinMappingReadDoFn(
              mappingTag, readTag)));

      return joined;
    }
  }

  protected static class KeyByContigId
  extends DoFn<ContigReadAlignment, KV<String, ContigReadAlignment>> {
    @Override
    public void processElement(ProcessContext c) {
      BowtieMapping mapping = c.element().getBowtieMapping();
      c.output(KV.of(mapping.getContigId().toString(), c.element()));
    }
  }

  /**
   * A transform to join ContigReadAlignment's with Contigs based on the bowtie mapping.
   */
  private static class JoinNodes extends PTransform<PCollectionTuple, PCollection<ContigReadAlignment>> {
    final public TupleTag<GraphNodeData> nodeTag;
    final public TupleTag<ContigReadAlignment> alignmentTag;

    private class JoinNodesDoFn extends DoFn<KV<String, CoGbkResult>, ContigReadAlignment> {
      @Override
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> e = c.element();
        Iterable<ContigReadAlignment> alignmentIter = e.getValue().getAll(
            alignmentTag);
        Iterable<GraphNodeData> nodeIter = e.getValue().getAll(nodeTag);
        for (ContigReadAlignment alignment : alignmentIter) {
          for (GraphNodeData nodeData : nodeIter) {
            ContigReadAlignment newAlignment = SpecificData.get().deepCopy(
                alignment.getSchema(), alignment);
            newAlignment.setGraphNode(SpecificData.get().deepCopy(
                nodeData.getSchema(), nodeData));
            c.output(newAlignment);
          }
        }
      }
    }

    public JoinNodes(
        TupleTag<GraphNodeData> nodeTag,
        TupleTag<ContigReadAlignment> alignmentTag) {
      this.nodeTag = nodeTag;
      this.alignmentTag = alignmentTag;
    }

    @Override
    public PCollection<ContigReadAlignment> apply(PCollectionTuple inputTuple) {
      PCollection<ContigReadAlignment> alignments = inputTuple.get(alignmentTag);
      PCollection<GraphNodeData> nodes = inputTuple.get(nodeTag);

      PCollection<KV<String, ContigReadAlignment>> keyedAlignments =
          alignments.apply(ParDo.of(new KeyByContigId()))
          .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroSpecificCoder.of(ContigReadAlignment.class)));

      PCollection<KV<String, GraphNodeData>> keyedNodes =
          nodes.apply(ParDo.of(new GraphNodeTransforms.KeyByNodeId())).setCoder(
              KvCoder.of(
                  StringUtf8Coder.of(),
                  AvroSpecificCoder.of(GraphNodeData.class)));

      PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(alignmentTag, keyedAlignments)
          .and(nodeTag, keyedNodes)
          .apply(CoGroupByKey.<String>create());


      PCollection<ContigReadAlignment> joined =
          coGbkResultCollection.apply(ParDo.of(new JoinNodesDoFn()));

        return joined;
    }
  }

  @Override
  public PCollection<ContigReadAlignment> apply(PCollectionTuple inputTuple) {
    PCollection<GraphNodeData> nodes = inputTuple.get(nodeTag);
    PCollection<BowtieMapping> mappings = inputTuple.get(mappingTag);
    PCollection<Read> reads = inputTuple.get(readTag);

    JoinMappingsAndReadsTransform joinMappingsAndReads =
        new JoinMappingsAndReadsTransform(mappingTag, readTag);

    PCollection<ContigReadAlignment> readMappingsPair =
        joinMappingsAndReads.apply(PCollectionTuple
            .of(mappingTag, mappings)
            .and(readTag, reads));


    JoinNodes joinNodes = new JoinNodes(nodeTag, alignmentTag);
    PCollection<ContigReadAlignment> joined = joinNodes.apply(PCollectionTuple
        .of(alignmentTag, readMappingsPair)
        .and(nodeTag, nodes));


    return joined;
  }
}
