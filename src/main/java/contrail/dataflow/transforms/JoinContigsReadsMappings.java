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

import java.util.ArrayList;

import org.apache.avro.specific.SpecificData;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import contrail.dataflow.BowtieMappingTransforms;
import contrail.dataflow.GraphNodeTransforms;
import contrail.dataflow.ReadTransforms;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigReadMappings;
import contrail.scaffolding.MappingReadPair;
import contrail.sequences.Read;

/**
 * Transforms for joining the contigs, bowtie alignments, and reads.
 */
//TODO(jlewi): Rename to something like MapReadsToContigs
public class JoinContigsReadsMappings
    extends PTransform<PCollectionTuple, PCollection<ContigReadMappings>> {
  final public TupleTag<contrail.graph.GraphNodeData> nodeTag = new TupleTag<>();
  final public TupleTag<BowtieMapping> mappingTag = new TupleTag<>();
  final public TupleTag<Read> readTag = new TupleTag<>();

  final private TupleTag<MappingReadPair> mappingReadPairTag =
      new TupleTag<>();

  /**
   * A transform for joining the bowtie mappings with the reads.
   */
  private static class JoinMappingsAndReadsTransform extends
      PTransform<PCollectionTuple, PCollection<MappingReadPair>> {
    final public TupleTag<BowtieMapping> mappingTag;
    final public TupleTag<Read> readTag;

    private class JoinMappingReadDoFn
        extends DoFn<KV<String, CoGbkResult>, MappingReadPair> {
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
            MappingReadPair pair = new MappingReadPair();
            pair.setBowtieMapping(SpecificData.get().deepCopy(
                mapping.getSchema(), mapping));
            pair.setRead(SpecificData.get().deepCopy(
                read.getSchema(), read));
            c.output(pair);
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
    public PCollection<MappingReadPair> apply(PCollectionTuple inputTuple) {
      PCollection<BowtieMapping> mappings = inputTuple.get(mappingTag);
      PCollection<Read> reads = inputTuple.get(readTag);
      PCollection<KV<String, BowtieMapping>> keyedMappings =
          mappings.apply(ParDo.of(new BowtieMappingTransforms.KeyByReadId()))
          .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroCoder.of(BowtieMapping.class)));
      PCollection<KV<String, Read>> keyedReads =
          reads.apply(ParDo.of(new ReadTransforms.KeyByReadIdDo())).setCoder(
              KvCoder.of(
                  StringUtf8Coder.of(),
                  AvroCoder.of(Read.class)));

      PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(mappingTag, keyedMappings)
          .and(readTag, keyedReads)
          .apply(CoGroupByKey.<String>create());


      PCollection<MappingReadPair> joined =
          coGbkResultCollection.apply(ParDo.of(new JoinMappingReadDoFn(
              mappingTag, readTag))).setCoder(
                  AvroCoder.of(MappingReadPair.class));

      return joined;
    }
  }

  protected static class KeyMappingPairByContigId
      extends DoFn<MappingReadPair, KV<String, MappingReadPair>> {
    @Override
    public void processElement(ProcessContext c) {
      BowtieMapping mapping = c.element().getBowtieMapping();
      c.output(KV.of(mapping.getContigId().toString(), c.element()));
    }
  }

  /**
   * A transform to join ContigReadAlignment's with Contigs based on the bowtie mapping.
   */
  private static class JoinNodes
        extends PTransform<PCollectionTuple, PCollection<ContigReadMappings>> {
    final public TupleTag<GraphNodeData> nodeTag;
    final public TupleTag<MappingReadPair> mappingReadPairTag;

    private class JoinNodesDoFn
        extends DoFn<KV<String, CoGbkResult>, ContigReadMappings> {
      @Override
      public void processElement(ProcessContext c) {
        KV<String, CoGbkResult> e = c.element();
        Iterable<MappingReadPair> mappingIter = e.getValue().getAll(
            mappingReadPairTag);
        Iterable<GraphNodeData> nodeIter = e.getValue().getAll(nodeTag);
        for (GraphNodeData nodeData : nodeIter) {
          GraphNode node = new GraphNode(nodeData);
          // TODO(jlewi): There should be at most 1 GraphNodeData.
          // If there are more its an error of some sorts.
          ContigReadMappings contigMappings = new ContigReadMappings();
          contigMappings.setGraphNode(node.clone().getData());
          contigMappings.setMappings(new ArrayList<MappingReadPair>());
          for (MappingReadPair mapping : mappingIter) {
              contigMappings.getMappings().add(SpecificData.get().deepCopy(
                  mapping.getSchema(), mapping));
          }
          c.output(contigMappings);
        }
      }
    }

    public JoinNodes(
        TupleTag<GraphNodeData> nodeTag,
        TupleTag<MappingReadPair> mappingReadPairTag) {
      this.nodeTag = nodeTag;
      this.mappingReadPairTag = mappingReadPairTag;
    }

    @Override
    public PCollection<ContigReadMappings> apply(PCollectionTuple inputTuple) {
      PCollection<MappingReadPair> mappings = inputTuple.get(mappingReadPairTag);
      PCollection<GraphNodeData> nodes = inputTuple.get(nodeTag);

      PCollection<KV<String, MappingReadPair>> keyedMappings =
          mappings.apply(ParDo.of(new KeyMappingPairByContigId()))
          .setCoder(KvCoder.of(
              StringUtf8Coder.of(),
              AvroCoder.of(MappingReadPair.class)));

      PCollection<KV<String, GraphNodeData>> keyedNodes =
          nodes.apply(ParDo.of(new GraphNodeTransforms.KeyByNodeId())).setCoder(
              KvCoder.of(
                  StringUtf8Coder.of(),
                  AvroCoder.of(GraphNodeData.class)));

      PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
          KeyedPCollectionTuple.of(mappingReadPairTag, keyedMappings)
          .and(nodeTag, keyedNodes)
          .apply(CoGroupByKey.<String>create());


      PCollection<ContigReadMappings> joined =
          coGbkResultCollection.apply(ParDo.of(new JoinNodesDoFn()));

        return joined;
    }
  }

  @Override
  public PCollection<ContigReadMappings> apply(PCollectionTuple inputTuple) {
    PCollection<GraphNodeData> nodes = inputTuple.get(nodeTag);
    PCollection<BowtieMapping> mappings = inputTuple.get(mappingTag);
    PCollection<Read> reads = inputTuple.get(readTag);

    JoinMappingsAndReadsTransform joinMappingsAndReads =
        new JoinMappingsAndReadsTransform(mappingTag, readTag);

    PCollection<MappingReadPair> mappingReadPairs =
        joinMappingsAndReads.apply(PCollectionTuple
            .of(mappingTag, mappings)
            .and(readTag, reads));


    JoinNodes joinNodes = new JoinNodes(nodeTag, mappingReadPairTag);
    PCollection<ContigReadMappings> joined = joinNodes.apply(PCollectionTuple
        .of(mappingReadPairTag, mappingReadPairs)
        .and(nodeTag, nodes)).setCoder(
            AvroCoder.of(ContigReadMappings.class));

    return joined;
  }
}
