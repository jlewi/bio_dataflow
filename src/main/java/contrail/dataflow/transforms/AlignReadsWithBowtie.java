import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.transforms.AsIterable;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SeqDo;
import com.google.cloud.dataflow.sdk.transforms.SeqDo.SeqDoFn;
import com.google.cloud.dataflow.sdk.util.UserCodeException;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PObject;
import com.google.cloud.dataflow.sdk.values.PObjectTuple;
import com.google.cloud.dataflow.sdk.values.PObjectValueTuple;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import contrail.dataflow.BowtieInput;
import contrail.dataflow.RunBowtie;
import contrail.graph.GraphNodeData;
import contrail.scaffolding.BowtieMapping;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.sequences.FastaRecord;
import contrail.sequences.QuakeReadCorrection;
import contrail.sequences.Read;
import contrail.util.DockerMappedPath;
import contrail.util.FileHelper;
import contrail.util.ShellUtil;

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

/**
 * A transform for aligning reads with bowtie.
 *
 * input: A tuple of pcollection containing the reads and contigs.
 * output: A pcollection of bowtie alignments.
 *
 * This is a multi step transform.
 * 1. The reads are turned into short sequences suitable for bowtie using a
 *    transform.
 * 2. The reads are passed to a SeqDo function which runs bowtie.
 *    The SeqDo writes the reads to files and then runs bowtie-index in a
 *    container to build the index. We then run bowtie in a container
 *    to align the reads.
 *
 * N.B. 2014-09-22 this is a work in progress. Look at RunBowtie.java for
 * similar code.
 */
public class AlignReadsWithBowtie extends PTransform<PCollectionTuple, PCollection<BowtieMapping>> {
  private static final Logger sLogger = Logger.getLogger(
      AlignReadsWithBowtie.class);
  final public TupleTag<Read> readTag = new TupleTag<>();
  final public TupleTag<GraphNodeData> nodeTag = new TupleTag<>();

  final public TupleTag<Iterable<Read>> iterableReadsTag =
      new TupleTag<>();

  final public TupleTag<Iterable<Read>> iterableReferenceTag =
      new TupleTag<>();

  private int shortenedLength;

  private static class ShortenReads extends DoFn<Read, Read> {
    private int shortenedLength;

    public ShortenReads(int shortenedLength) {
      this.shortenedLength = shortenedLength;
    }

    public List<String> shorten(String sequence) {
      List<String> pieces = new ArrayList<String>();
      int readLength = sequence.length();
      if (readLength < shortenedLength) {
        return pieces;
      }

      // Get the first part
      pieces.add(sequence.substring(0, shortenedLength));
      // Get the end.
      pieces.add(sequence.substring(sequence.length() - shortenedLength));

      // Get the middle part.
      int start = (int) Math.floor(sequence.length()/2.0 - shortenedLength/2.0);
      pieces.add(sequence.substring(start, start + shortenedLength));
    }

    @Override
    public void processElement(ProcessContext c) {
      Read read = c.element();

      if (read.getFasta() != null) {
        FastaRecord fasta = read.getFasta();
        String sequence = fasta.getRead().toString();
        List<String> pieces = shorten(sequence);
        for (String subSequence : pieces) {
          Read shortRead = new Read();
          FastaRecord shortFasta = new FastaRecord();
          read.setFasta(shortFasta);
          shortFasta.setId(fasta.getId());
          shortFasta.setRead(subSequence);
        }
      }
      if (read.getFastq() != null) {
        FastQRecord fastq = read.getFastq();
        String sequence = fastq.getRead().toString();
        String qValue = fastq.getQvalue().toString();
        if (sequence.length() != qValue.length()) {
          // TODO(jlewi): Increment a counter to indicate we are skipping this
          // read.
        } else {
          List<String> pieces = shorten(sequence);
          List<String> shortQValues = shorten(qValue);
          // TODO(jlewi): Check pieces and shortQValue are the same length.

          for (int index = 0; index < pieces.size(); ++index) {
            Read shortRead = new Read();
            FastQRecord shortFastq = new FastQRecord();
            read.setFastq(shortFastq);
            shortFastq.setId(fastq.getId());
            shortFastq.setRead(pieces.get(index));
            shortFastq.setQvalue(shortQValues.get(index));
          }
        }
      }
    }
  }

  /**
   * A DoFn to build the bowtie index.
   *
   * The input is a tuple consisting of the reads to build the index from.
   * These should be Fasta records represented in avro Read records.
   *
   * The output is a PObject containing a BowtieIndexInfo avro record
   * describing the index.
   *
   * The actual index files are copied to GCS so that they can be passed to
   * other steps.
   */
  private static class BuildBowtieIndex extends SeqDoFn {
    private TupleTag<Iterable<Read>> iterableReadsTag;

    /**
     * @param readTag: Tag to use for the reads.
     */
    public BuildBowtieIndex(TupleTag<Iterable<Read>> iterableReadsTag) {
      this.iterableReadsTag = iterableReadsTag;
    }


    @Override
    public PObjectValueTuple process(PObjectValueTuple inputs) {
      // Write the reads to files.
      Iterable<Read> iterableReads = inputs.get(iterableReadsTag);
      String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();

      // containerDir is the directory inside the container which is mapped
      // to a temporary directory on the VM. We use that directory to make
      // the results of the index available to be copied to GCS.
      String containerDir = "/container_data";
      DockerMappedPath refFile = DockerMappedPath.create(
          tempDir, containerDir, "ref.fasta");

      try {
        FileOutputStream refStream = new FileOutputStream(refFile.hostPath);
        for (Read read : iterableReads) {
          FastUtil.writeFastARecord(refStream, read.getFasta());
        }
        refStream.close();

      } catch (IOException e) {
        e.printStackTrace();
        throw new UserCodeException(e);
      }

      DockerMappedPath containerIndexFile = DockerMappedPath.create(
          tempDir, containerDir, "bowtie.index");

      String imageName = "contrail/bowtie";

      ProcessBuilder builder = new ProcessBuilder(
          "docker", "run", "-t",
          "-v", tempDir + ":" + containerDir,
          imageName,
          "/git_bowtie/bowtie-build",
          refFile.containerPath,
          containerIndexFile.containerPath);
      ShellUtil.runProcess(
          builder, "bowtie-index", "", sLogger, null);

      // TODO(jlewi): Copy the index to GCS.
      return null;
    }
  }

  public AlignReadsWithBowtie(int shortenedLength) {
    this.shortenedLength = shortenedLength;
  }

  @Override
  public PCollection<BowtieMapping> apply(PCollectionTuple input) {
    PCollection<Read> reads = input.get(readTag);
    PCollection<GraphNodeData> nodes = input.get(nodeTag);

    PCollection<Read> shortenedReads = reads.apply(ParDo.of(new ShortenReads(shortenedLength)));

    // TODO(jlewi): Need to define the actual DoFn to GraphNodeToRead
    // Convert the GraphNode to a Fasta sequence represented as a Read record.
    PCollection<Read> contigs = nodes.apply(ParDo.of(new GraphNodeToRead()));

    // Is materializing the reads with as iterable and them passing them to
    // BuildBowtieIndex the best(only?) way to make the
    // reads available to the BowtieBuildIndex DoFn?
    PObject<Iterable<Read>> iterableReads = shortenedReads.apply(
        AsIterable.<Read>create());

    PObject<Iterable<Read>> iterableContigs = contigs.apply(
        AsIterable.<Read>create());

    PObjectTuple buildIndexInput = PObjectTuple.of(
        iterableReferenceTag, iterableContigs)

        // Build the index and copy it to GCS.
        PObject<IndexInfo> index = buildIndexInput.apply(
            SeqDo.of(new BuildBowtieIndex(iterableReferenceTag)));

    // Align the reads.
    PCollection<BowtieMapping) aligned = shortenedReads.apply(
        ParDo.withSideInput(new AlignReads(), index));

    return aligned;
  }
}
