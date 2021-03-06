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
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

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
 * Join the bowtie mappings, reads, and contigs.
 *
 * The contigs are also provided and we include only those reads which align
 * to a subset of contigs.
 */
public class JoinMappingsAndReads extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      JoinMappingsAndReads.class);

  private final TupleTag<ContigReadAlignment> alignmentTag = new TupleTag<>();
  private final TupleTag<GraphNodeData> nodeTag = new TupleTag<>();

  private final TupleTag<BowtieMapping> mappingTag = new TupleTag<>();
  private final TupleTag<Read> readTag = new TupleTag<>();

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
        "min_contig_length", "The minimum length of contigs to include.",
         Integer.class, 0));

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

  protected static class JoinMappingReadDoFn
      extends DoFn<KV<String, CoGbkResult>, ContigReadAlignment> {
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

  protected PCollection<ContigReadAlignment> joinMappingsAndReads(
      PCollection<BowtieMapping> mappings, PCollection<Read> reads) {
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


    PCollection<ContigReadAlignment> joined =
        coGbkResultCollection.apply(ParDo.of(new JoinMappingReadDoFn(
            mappingTag, readTag)));

    return joined;
  }

  protected static class KeyByContigId
  extends DoFn<ContigReadAlignment, KV<String, ContigReadAlignment>> {
    @Override
    public void processElement(ProcessContext c) {
      BowtieMapping mapping = c.element().getBowtieMapping();
      c.output(KV.of(mapping.getContigId().toString(), c.element()));
    }
  }

  protected static class JoinNodesDoFn
      extends DoFn<KV<String, CoGbkResult>, ContigReadAlignment> {
    private final TupleTag<ContigReadAlignment> alignmentTag;
    private final TupleTag<GraphNodeData> nodeTag;

    public JoinNodesDoFn(
        TupleTag<ContigReadAlignment> alignmentTag,
        TupleTag<GraphNodeData> nodeTag) {
      this.alignmentTag = alignmentTag;
      this.nodeTag = nodeTag;
    }

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

  /**
   * Join the graph nodes with the ContigReadAlignments.
   * @param mappings
   * @param reads
   * @return
   */
  protected PCollection<ContigReadAlignment> joinNodes(
      PCollection<ContigReadAlignment> alignments,
      PCollection<GraphNodeData> nodes) {
    PCollection<KV<String, ContigReadAlignment>> keyedAlignments =
        alignments.apply(ParDo.of(new KeyByContigId()))
        .setCoder(KvCoder.of(
            StringUtf8Coder.of(),
            AvroCoder.of(ContigReadAlignment.class)));

    PCollection<KV<String, GraphNodeData>> keyedNodes =
        nodes.apply(ParDo.of(new GraphNodeTransforms.KeyByNodeId())).setCoder(
            KvCoder.of(
                StringUtf8Coder.of(),
                AvroCoder.of(GraphNodeData.class)));

    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
        KeyedPCollectionTuple.of(alignmentTag, keyedAlignments)
        .and(nodeTag, keyedNodes)
        .apply(CoGroupByKey.<String>create());


    PCollection<ContigReadAlignment> joined =
        coGbkResultCollection.apply(ParDo.of(new JoinNodesDoFn(
            alignmentTag, nodeTag)));

    return joined;
  }

  /**
   * Output the contig as a string representing the fasta record.
   */
  protected static class OutputContigAsFastaDo
      extends DoFn<GraphNodeData, String> {
    @Override
    public void processElement(ProcessContext c) {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      FastaRecord record = new FastaRecord();
      GraphNode node = new GraphNode(c.element());
      record.setId(node.getNodeId().toString());
      record.setRead(node.getSequence().toString());
      try {
        FastUtil.writeFastARecord(stream, record);
        String output = new String(stream.toByteArray(),"UTF-8");
        c.output(output);
      } catch (IOException e) {
       sLogger.info("Exception occurred writing fasta record:\n" +
                    e.toString());
      }
    }
  }

  /**
   * Output the read as a string representing the fastq record.
   */
  protected static class OutputReadAsFastqDo
      extends DoFn<ContigReadAlignment, String> {
    @Override
    public void processElement(ProcessContext c) {
      ContigReadAlignment alignment = c.element();
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      try {
        FastUtil.writeFastQRecord(stream, alignment.getRead().getFastq());
        String output = new String(stream.toByteArray(),"UTF-8");
        c.output(output);
      } catch (IOException e) {
       sLogger.info("Exception occurred writing fastq record:\n" +
                    e.toString());
      }
    }
  }

  /**
   * Filter the nodes.
   */
  protected static class FilterNodesByLengthDoFn
      extends DoFn<GraphNodeData, GraphNodeData> {
    private final int minLength;
    /**
     * @param minLength: The minimum length of nodes to accept.
     */
    public FilterNodesByLengthDoFn(int minLength) {
      this.minLength = minLength;
    }

    @Override
    public void processElement(ProcessContext c) {
      GraphNodeData nodeData = c.element();
      GraphNode node = new GraphNode(nodeData);

      if (node.getData().getSequence().getLength() < minLength) {
        return;
      }
      c.output(nodeData);
    }
  }

  /**
   * Remove all BowtieMappings which don't correspond to one of the contig
   * ids supplied as a secondary input.
   */
  private static class FilterBowtieMappingsByContigId extends
    DoFn<BowtieMapping, BowtieMapping> {

    private final TupleTag<Iterable<String>> contigIdsTag;

    // This will be initialized in StartBatch so it shouldn't be serialized.
    private transient HashSet<String> contigIds;

    public FilterBowtieMappingsByContigId(
        TupleTag<Iterable<String>> contigIdsTag) {
      this.contigIdsTag= contigIdsTag;
    }

    @Override
    public void startBundle(DoFn.Context c) {
        throw new RuntimeException(
            "The code no longer appears to work with the latest version of " +
            "the Dataflow sdk. Need tof igure out how to get side inputs");
//      Iterable<String> iterableIds =
//          (Iterable<String>) c.sideInput(contigIdsTag);
//      contigIds = new HashSet<String>();
//      for (String id : iterableIds) {
//        contigIds.add(id);
//      }
    }

    @Override
    public void processElement(ProcessContext c) {
      BowtieMapping mapping = c.element();
      if (!contigIds.contains(mapping.getContigId().toString())) {
        return;
      }
      c.output(mapping);
    }
  }

  /**
   * Join the mappings with the provided contig ids and remove any mappings
   * for which the contig the mapping belongs to is not in the list of
   * mappings.
   * @param mappings
   * @param contigIds
   * @return
   */
  private PCollection<BowtieMapping> filterMappingsByContigs(
      PCollection<BowtieMapping> mappings, PCollection<String> contigIds) {
    // Pass the contigIds as a side input to the mapping filter function.
    throw new RuntimeException(
        "2015-01-18 the code below needs to be updated to work with the " +
        "latest SDK.");
//    PObject<Iterable<String>> iterableIds =
//        contigIds.apply(AsIterable.<String>create());
//
//    final TupleTag<Iterable<String>> contigIdsTag = new TupleTag<>();
//    PObjectTuple sideTuple = PObjectTuple.of(contigIdsTag, iterableIds);
//    PCollection<BowtieMapping> filteredMappings =
//        mappings.apply(ParDo.named("FilterMappings")
//            .withSideInputs(sideTuple)
//            .of(new FilterBowtieMappingsByContigId(contigIdsTag)));

    // return filteredMappings;
  }
//  /**
//   * Compute statistics about the alignment
//   */
//  protected static class ComputeMismatchDo
//    extends DoFn<ContigReadAlignment, ContigReadAlignment> {
//    @Override
//    public void processElement(ProcessContext c) {
//      ContigReadAlignment alignment = c.element();
//      BowtieMapping mapping = alignment.getBowtieMapping();
//      GraphNode node = new GraphNode(alignment.getGraphNode());
//
//      Sequence contigSequence = node.getSequence();
//      int contigStart = mapping.getContigEnd();
//      int contigEnd = mapping.getContigEnd();
//
//      Sequence readSequence = new Sequence(
//          alignment.getRead().getFastq().getRead().toString(),
//          DNAAlphabetFactory.create());
//
//
//      // How do we deal with the fact the read would have been shortened.
//      // Suppose we have contig: ACTGAA
//      // RC: TTCAGT
//      // Suppose read is TCAGT but we shorten it to TCAGT.
//      // RC(TC) = GA  -> end = 3, start =4
//      // RC(read) = ACTGA
//      // So if mapping.getContigStart() > mapping.getContig(end) align
//      // the RC of the read reading right to left, otherwise align the
//      // read going left to right.
//      boolean alignedToRC = (mapping.getContigStart() > mapping.getContigEnd());
//      Sequence contigOverlap = null;
//      Sequence readOverlap = null;
//      if (alignedToRC) {
//        // Contig is aligned to reverse complement.
//        // See.
//        // http://bowtie-bio.sourceforge.net/manual.shtml#default-bowtie-output
//        // read ~ = RC(contig[start:end]).
//        // So we read right to left starting with mapping.getContigStart().
//        readSequence = DNAUtil.reverseComplement(readSequence);
//        int end = Math.min(
//            contigSequence.size(),
//            mapping.getContigStart() + readSequence.size());
//        contigOverlap = contigSequence.subSequence(
//            mapping.getContigStart(), end);
//
//        // Trim the read if necessary.
//        readOverlap = readSequence.subSequence(
//            0, Math.min(readSequence.size(), contigOverlap.size()));
//      } else {
//        // Since read is aligned to forward strand read the overlap left to
//        // right.
//        int end = Math.min(
//            contigSequence.size(),
//            mapping.getContigStart() + readSequence.size());
//        contigOverlap = contigSequence.subSequence(
//            mapping.getContigStart(), end);
//
//        // Trim the read if necessary.
//        readOverlap = readSequence.subSequence(
//            0, Math.min(readSequence.size(), contigOverlap.size()));
//      }
//
//      int numMismatches = 0;
//      if (contigOverlap.size() != readOverlap.size()) {
//        sLogger.error("Read and contig overlap aren't the same. This should " +
//                      "not happen.");
//      }
//
//      // TODO(jeremy@lewi.us): This isn't accounting for inserts/deletions.
//      for (int i = 0; i < readOverlap.size(); ++i) {
//        if (contigOverlap.at(i) != readOverlap.at(i)) {
//          ++numMismatches;
//        }
//      }
//    }
//  }

  protected static class BuildResult {
    PCollection<ContigReadAlignment> joined;
    PCollection<GraphNodeData> filteredContigs;
  }

  protected BuildResult buildPipeline(
      Pipeline p,
      PCollection<GraphNodeData> nodes,
      PCollection<BowtieMapping> mappings,
      PCollection<Read> reads) {
    int minLength = (Integer) stage_options.get("min_contig_length");
    nodes = nodes.apply(ParDo.of(new FilterNodesByLengthDoFn(minLength)));

    // Get the ids of the contigs we are keeping.
    PCollection<String> contigIds = nodes.apply(ParDo.of(
        new GraphNodeDoFns.GetNodeId()));

    // Filter the mappings so we only include the mappings for the contigs
    // we want.
    mappings = filterMappingsByContigs(mappings, contigIds);

    // TODO(jlewi): We should rewrite this using the transforms in ContigReadJoinTransforms.
    PCollection<ContigReadAlignment> readMappingsPair = joinMappingsAndReads(
        mappings, reads);

    PCollection<ContigReadAlignment> nodeReadsMappings = joinNodes(
        readMappingsPair, nodes);

    BuildResult result = new BuildResult();
    result.joined = nodeReadsMappings;
    result.filteredContigs = nodes;
    return result;
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

    PipelineOptions options = PipelineOptionsFactory.create();
    DataflowParameters.setPipelineOptions(stage_options, options);

    Pipeline p = Pipeline.create(options);

    DataflowUtil.registerAvroCoders(p);

    PCollection<GraphNodeData> nodes = p.apply(
        AvroIO.Read.from(contigsPath).withSchema(GraphNodeData.class));


    PCollection<Read> reads = p.apply(
        AvroIO.Read.from(readsPath).withSchema(Read.class));

    PCollection<BowtieMapping> mappings = p.apply(
        AvroIO.Read.from(bowtieAlignmentsPath).withSchema(
            BowtieMapping.class));

    BuildResult buildResult = buildPipeline(p, nodes, mappings, reads);
    PCollection<ContigReadAlignment> nodeReadsMappings = buildResult.joined;

    String fastaOutputs = outputPath + "contigs.fasta@*";
    PCollection<String> fastaContigs = buildResult.filteredContigs.apply(
        ParDo.of(new OutputContigAsFastaDo()));

    fastaContigs.apply(TextIO.Write.named("WriteContigs").to(fastaOutputs));

    PCollection<String> outReads = nodeReadsMappings.apply(ParDo.of(
        new OutputReadAsFastqDo()));

    String fastqOutputs = outputPath + "reads.fastq@*";
    outReads.apply(TextIO.Write.named("WriteContigs").to(fastqOutputs));

    sLogger.info("JoinMappingReadDoFn.mappingTag: " + mappingTag.toString());
    sLogger.info("JoinMappingReadDoFn.readTag: " + readTag.toString());
    sLogger.info("JoineNodesDoFn.alignmentTag: " + alignmentTag.toString());
    sLogger.info("JoineNodesDoFn.nodeTag: " + nodeTag.toString());
    p.run();

    sLogger.info("Output written to: " + outputPath);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new JoinMappingsAndReads(), args);
    System.exit(res);
  }
}
