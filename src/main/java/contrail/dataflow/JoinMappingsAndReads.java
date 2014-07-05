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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollections;
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

public class JoinMappingsAndReads extends NonMRStage {
  private static final Logger sLogger = Logger.getLogger(
      JoinMappingsAndReads.class);

  public static final TupleTag<ContigReadAlignment> alignmentTag =
      new TupleTag<>();
  public static final TupleTag<GraphNodeData> nodeTag = new TupleTag<>();

  public static final TupleTag<BowtieMapping> mappingTag = new TupleTag<>();
  public static final TupleTag<Read> readTag = new TupleTag<>();

  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition bowtieAlignments =
        new ParameterDefinition(
            "bowtie_alignments",
            "The hdfs path to the avro files containing the alignments " +
            "produced by bowtie of the reads to the contigs.",
            String.class, null);

    ParameterDefinition graphPath =
        new ParameterDefinition(
            "contigs", "The glob on the hadoop filesystem to the avro " +
            "files containing the GraphNodeData records representing the " +
            "graph.", String.class, null);

    ParameterDefinition readsPath =
        new ParameterDefinition(
            "reads", "The glob on the hadoop filesystem to the avro " +
            "files containing the reads.", String.class, null);

    ParameterDefinition project =
        new ParameterDefinition(
            "project", "The google cloud project to use when running on  " +
            "Dataflow.", String.class, null);

    ParameterDefinition stagingLocation =
        new ParameterDefinition(
            "stagingLocation", "Location on GCS where files should be staged.",
            String.class, null);

    ParameterDefinition dataflowEndpoint =
        new ParameterDefinition(
            "dataflowEndpoint", "Dataflow endpoint",
            String.class, null);

    ParameterDefinition apiRootUrl =
        new ParameterDefinition(
            "apiRootUrl", "Root url.",
            String.class, null);

    for (ParameterDefinition def : ContrailParameters.getInputOutputPathOptions() ){
      if (def.getName().equals("outputpath")) {
        defs.put(def.getName(), def);
      }
    }

    ParameterDefinition pipelineRunner =
        new ParameterDefinition(
            "runner", "The pipeline runner to use.",
            String.class, "DirectPipelineRunner");

    defs.put(bowtieAlignments.getName(), bowtieAlignments);
    defs.put(graphPath.getName(), graphPath);
    defs.put(readsPath.getName(), readsPath);
    defs.put(pipelineRunner.getName(), pipelineRunner);
    defs.put(project.getName(), project);
    defs.put(stagingLocation.getName(), stagingLocation);
    defs.put(dataflowEndpoint.getName(), dataflowEndpoint);
    defs.put(apiRootUrl.getName(), apiRootUrl);

    return Collections.unmodifiableMap(defs);
  }

  protected PCollection<GraphNodeData> readGraphNodes(Pipeline p, String path) {
    GCSAvroFileSplit split = new GCSAvroFileSplit();
    split.setPath(path);

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);
    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));

    return inputs
        .apply(ParDo.of(new ReadAvroSpecificDoFn<GraphNodeData>(
            GraphNodeData.class)))
        .setCoder(AvroSpecificCoder.of(GraphNodeData.class));
  }

  protected PCollection<Read> readReads(
      Pipeline p, String path) {
    GCSAvroFileSplit split = new GCSAvroFileSplit();

    split.setPath(path);

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);
    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));

    return inputs
        .apply(ParDo.of(new ReadAvroSpecificDoFn<Read>(Read.class)))
        .setCoder(AvroSpecificCoder.of(Read.class));
  }

  protected PCollection<BowtieMapping> readMappings(Pipeline p, String path) {
    GCSAvroFileSplit split = new GCSAvroFileSplit();

    split.setPath(path);

    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();

    splits.add(split);
    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits));

    return inputs
        .apply(ParDo.of(new ReadAvroSpecificDoFn<BowtieMapping>(
            BowtieMapping.class)))
        .setCoder(AvroSpecificCoder.of(BowtieMapping.class));
  }

  protected static class JoinMappingReadDoFn
  extends DoFn<KV<String, CoGbkResult>, ContigReadAlignment> {
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
        KeyedPCollections.of(mappingTag, keyedMappings)
        .and(readTag, keyedReads)
        .apply(CoGroupByKey.<String>create());


    PCollection<ContigReadAlignment> joined =
        coGbkResultCollection.apply(ParDo.of(new JoinMappingReadDoFn()));

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
            AvroSpecificCoder.of(ContigReadAlignment.class)));

    PCollection<KV<String, GraphNodeData>> keyedNodes =
        nodes.apply(ParDo.of(new GraphNodeTransforms.KeyByNodeId())).setCoder(
            KvCoder.of(
                StringUtf8Coder.of(),
                AvroSpecificCoder.of(GraphNodeData.class)));

    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
        KeyedPCollections.of(alignmentTag, keyedAlignments)
        .and(nodeTag, keyedNodes)
        .apply(CoGroupByKey.<String>create());


    PCollection<ContigReadAlignment> joined =
        coGbkResultCollection.apply(ParDo.of(new JoinNodesDoFn()));

    return joined;
  }

  /**
   * Output the contig as a string representing the fasta record.
   */
  protected static class OutputContigAsFastaDo
      extends DoFn<ContigReadAlignment, String> {
    @Override
    public void processElement(ProcessContext c) {
      ContigReadAlignment alignment = c.element();
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      FastaRecord record = new FastaRecord();
      GraphNode node = new GraphNode(alignment.getGraphNode());
      record.setId(alignment.getGraphNode().getNodeId().toString());
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

  @Override
  protected void stageMain() {
    String readsPath = (String) stage_options.get("reads");
    String contigsPath = (String) stage_options.get("contigs");
    String bowtieAlignmentsPath = (String) stage_options.get(
        "bowtie_alignments");
    Date now = new Date();
    SimpleDateFormat formatter = new SimpleDateFormat("yyyymmdd-hhmmss");
    // N.B. We don't use FilenameUtils.concat because it messes up the URI
    // prefix.
    String outputPath = (String) stage_options.get("outputpath");
    if (!outputPath.endsWith("/")) {
      outputPath += "/";
    }
    outputPath += formatter.format(now);
    outputPath += "/";

    PipelineOptions options = new PipelineOptions();
    options.runner = (String) stage_options.get("runner");
    options.project = (String) stage_options.get("project");
    options.stagingLocation = (String) stage_options.get("stagingLocation");

    if (stage_options.get("dataflowEndpoint") != null) {
      options.dataflowEndpoint =
          (String) (stage_options.get("dataflowEndpoint"));
    }

    if (stage_options.get("apiRootUrl") != null) {
      options.apiRootUrl =
          (String) (stage_options.get("apiRootUrl"));
    }

    Pipeline p = Pipeline.create();

    DataflowUtil.registerAvroCoders(p);

    PCollection<GraphNodeData> nodes = readGraphNodes(p, contigsPath);
    PCollection<Read> reads = readReads(p, readsPath);
    PCollection<BowtieMapping> mappings = readMappings(p, bowtieAlignmentsPath);

    PCollection<ContigReadAlignment> readMappingsPair = joinMappingsAndReads(
        mappings, reads);

    PCollection<ContigReadAlignment> nodeReadsMappings = joinNodes(
        readMappingsPair, nodes);

    String fastaOutputs = outputPath + "contigs.fasta";
    PCollection<String> fastaContigs = nodeReadsMappings.apply(ParDo.of(
        new OutputContigAsFastaDo()));

    fastaContigs.apply(TextIO.Write.named("WriteContigs").to(fastaOutputs));

    PCollection<String> outReads = nodeReadsMappings.apply(ParDo.of(
        new OutputReadAsFastqDo()));

    String fastqOutputs = outputPath + "reads.fastq";
    outReads.apply(TextIO.Write.named("WriteContigs").to(fastqOutputs));

    sLogger.info("JoinMappingReadDoFn.mappingTag: " + mappingTag.toString());
    sLogger.info("JoinMappingReadDoFn.readTag: " + readTag.toString());
    sLogger.info("JoineNodesDoFn.alignmentTag: " + alignmentTag.toString());
    sLogger.info("JoineNodesDoFn.nodeTag: " + nodeTag.toString());
    p.run(PipelineRunner.fromOptions(options));

    sLogger.info("Output written to: " + outputPath);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new JoinMappingsAndReads(), args);
    System.exit(res);
  }
}
