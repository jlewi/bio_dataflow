/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.CompressedRead;
import contrail.ReadState;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.KMerEdge;
import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.FastQRecord;
import contrail.sequences.KMerReadTag;
import contrail.sequences.Read;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

/**
 * This stage constructs the initial graph based on KMers in the reads.
 *
 * The input is an avro file. The records in the avro file can either be
 * CompressedRead or FastQRecords.
 */
public class BuildGraphAvro extends MRStage {
  private static final Logger sLogger = Logger.getLogger(BuildGraphAvro.class);

  public static final Schema kmer_edge_schema = (new KMerEdge()).getSchema();
  public static final Schema graph_node_data_schema = (new GraphNodeData())
      .getSchema();

  /**
   * Define the schema for the mapper output. The keys will be a byte buffer
   * representing the compressed source KMer sequence. The value will be an
   * instance of KMerEdge.
   */
  public static final Schema MAP_OUT_SCHEMA = Pair.getPairSchema(
      Schema.create(Schema.Type.BYTES), kmer_edge_schema);

  /**
   * Define the schema for the reducer output. The keys will be a byte buffer
   * representing the compressed source KMer sequence. The value will be an
   * instance of GraphNodeData.
   */
  public static final Schema REDUCE_OUT_SCHEMA = graph_node_data_schema;

  /**
   * Construct the nodeId for a given sequence.
   *
   * We currently assign a nodeId based on the actual sequence to ensure
   * uniqueness.
   *
   * @param sequence
   * @return
   */
  public static String constructNodeIdForSequence(Sequence sequence) {
    // TODO(jlewi): We use the base64 representation this adds 33%
    // overhead to the packed bytes version.
    return sequence.toBase64();
  }

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    // Add options specific to this stage.
    ParameterDefinition max_reads = new ParameterDefinition("max_reads",
        "max reads starts per node.", Integer.class, new Integer(250));

    ParameterDefinition trim3 = new ParameterDefinition("TRIM3",
        "Chopped bases.", Integer.class, new Integer(0));

    ParameterDefinition trim5 = new ParameterDefinition("TRIM5",
        "Chopped bases.", Integer.class, new Integer(0));

    ParameterDefinition maxR5 = new ParameterDefinition("MAXR5", "Max R5.",
        Integer.class, new Integer(250));

    ParameterDefinition max_thread_reads = new ParameterDefinition(
        "MAXTHREADREADS", "Max thread reads.", Integer.class, new Integer(250));

    ParameterDefinition record_all_threads = new ParameterDefinition(
        "RECORD_ALL_THREADS", "Record all threads.", Boolean.class,
        new Boolean(false));

    for (ParameterDefinition def : new ParameterDefinition[] { max_reads,
        trim3, trim5, maxR5, max_thread_reads, record_all_threads }) {
      defs.put(def.getName(), def);
    }

    for (ParameterDefinition def : ContrailParameters
        .getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Construct the destination KMer in an edge using the source KMer, the last
   * base for the sequence, and the strands for the edge.
   *
   * The mapper does a micro optimization. Since, the two KMers overlap by K-1
   * bases we can construct the destination KMer from the source KMer and the
   * non-overlap region of the destination
   *
   * @param canonical_src : Canonical representation of the source KMer.
   * @param last_base : The non overlap region of the destination KMer.
   * @param strands : Which strands the source and destination kmer came from.
   * @param alphabet : The alphabet for the encoding.
   * @return: The destination KMer.
   */
  public static Sequence ConstructDestSequence(Sequence canonical_src,
      ByteBuffer last_base_byte, StrandsForEdge strands, Alphabet alphabet) {
    Sequence last_base = new Sequence(alphabet);
    last_base.readPackedBytes(last_base_byte.array(), 1);
    Sequence dest;

    int K = canonical_src.size();
    if (StrandsUtil.src(strands) == DNAStrand.FORWARD) {
      dest = canonical_src.subSequence(1, K);
    } else {
      dest = DNAUtil.reverseComplement(canonical_src).subSequence(1, K);
    }
    dest.add(last_base);
    return dest;
  }

  /**
   * This class contains the operations for preprocessing sequences.
   *
   * This object is instantiated once for each mapper and customizes the
   * operations based on the settings and the alphabet.
   *
   * We use a separate class so that its easy to unittest.
   */
  public static class SequencePreProcessor {
    private final int trim5;
    private final int trim3;

    private boolean hasTrimVal;
    private int trimVal;

    /**
     * @param alphabet - The alphabet.
     * @param trim5 - Number of bases to trim off the start
     * @param trim3 - Number of bases to trim off the end.
     */
    public SequencePreProcessor(Alphabet alphabet, int trim5, int trim3) {
      this.trim5 = trim5;
      this.trim3 = trim3;

      // Check if this alphabet has a character which should be removed from
      // both ends of the sequence.
      if (alphabet.hasLetter('N')) {
        hasTrimVal = true;
        trimVal = alphabet.letterToInt('N');
      } else {
        hasTrimVal = false;
      }
    }

    /**
     * Pre process a sequence.
     *
     * @param seq - The sequence to process
     * @return - The processed sequence.
     */
    public Sequence PreProcess(Sequence seq) {
      // Start and end will store the valid range of the sequence.
      int start = 0;
      int end = seq.size();

      // Hard chop a few bases off each end of the read
      if (trim5 > 0 || trim3 > 0) {
        start = trim5;
        end = seq.capacity() - trim3;
      }

      // Automatically trim Ns off the very ends of reads
      if (hasTrimVal) {
        while (end > 0 && seq.valAt(end) == trimVal) {
          end--;
        }

        while (start < seq.size() && seq.valAt(start) == trimVal) {
          start++;
        }
      }
      return seq.subSequence(start, end);
    }
  }

  /**
   * Mapper for BuildGraph. Class is public to facilitate unit-testing.
   */
  public static class BuildGraphMapper extends
      AvroMapper<Object, Pair<ByteBuffer, KMerEdge>> {
    private static int K = 0;

    private final Alphabet alphabet = DNAAlphabetFactory.create();
    private Sequence seq = new Sequence(DNAAlphabetFactory.create());
    private SequencePreProcessor preprocessor;

    private final KMerEdge node = new KMerEdge();
    private Pair<ByteBuffer, KMerEdge> outPair;

    private CharSequence readId;

    @Override
    public void configure(JobConf job) {
      BuildGraphAvro stage = new BuildGraphAvro();
      Map<String, ParameterDefinition> definitions = stage
          .getParameterDefinitions();
      K = (Integer) (definitions.get("K").parseJobConf(job));
      if (K <= 0) {
        throw new RuntimeException("K must be a positive integer");
      }
      int TRIM5 = (Integer) (definitions.get("TRIM5").parseJobConf(job));
      int TRIM3 = (Integer) (definitions.get("TRIM3").parseJobConf(job));
      ;

      preprocessor = new SequencePreProcessor(alphabet, TRIM5, TRIM3);
      outPair = new Pair<ByteBuffer, KMerEdge>(MAP_OUT_SCHEMA);
    }

    /*
     * Input (CompressedRead) - Each input is an instance of CompressedRead.
     *
     * Output (ByteBuffer, KMerEdge): The output key is a sequence of bytes
     * representing the compressed KMer for the source node. The value is an
     * instance of KMerEdge which contains all the information for an edge
     * originating from this source KMer.
     *
     * For each successive pair of k-mers in the read, we output two tuples;
     * where each tuple corresponds to the read coming from a different strand
     * of the sequence.
     */
    @Override
    public void map(Object inputRecord,
        AvroCollector<Pair<ByteBuffer, KMerEdge>> output, Reporter reporter)
        throws IOException {
      if (inputRecord instanceof CompressedRead) {
        CompressedRead compressed_read = (CompressedRead) inputRecord;
        seq.readPackedBytes(compressed_read.getDna().array(),
            compressed_read.getLength());
        readId = compressed_read.getId();
      } else if (inputRecord instanceof FastQRecord) {
        FastQRecord fastQRecord = (FastQRecord) inputRecord;
        readId = fastQRecord.getId();
        seq.readCharSequence(fastQRecord.getRead());
      } else if (inputRecord instanceof Read) {
        Read read = (Read) inputRecord;
        readId = read.getFastq().getId();
        seq.readCharSequence(read.getFastq().getRead());
      }

      seq = preprocessor.PreProcess(seq);

      // Check for short reads.
      if (seq.size() <= K) {
        reporter.incrCounter("Contrail", "reads_short", 1);
        return;
      }

      ReadState ustate;
      ReadState vstate;

      Set<String> seenmers = new HashSet<String>();

      int chunk = 0;

      int end = seq.size() - K;

      // Now emit the edges of the de Bruijn Graph.
      // Each successive kmer in the read is a node in the graph
      // and the edge connecting them is the (k-1) of overlap.
      // Since we don't know which strand the read came from, we need
      // to consider both the read and its reverse complement when generating
      // edges.
      for (int i = 0; i < end; i++) {
        // ukmer and vkmer are sequential KMers in the read.
        Sequence ukmer = seq.subSequence(i, i + K);
        Sequence vkmer = seq.subSequence(i + 1, i + 1 + K);

        // ukmer_start and vkmer_end are the base we need to add
        // to the source kmer in order to generate the destination KMer.
        Sequence ukmer_start = seq.subSequence(i, i + 1);
        Sequence vkmer_end = seq.subSequence(i + K, i + K + 1);
        ukmer_start = DNAUtil.reverseComplement(ukmer_start);

        // Construct the canonical representation of each kmer.
        // This ensures that a kmer coming from the forward and reverse
        // strand are both represented using the same node.
        Sequence ukmer_canonical = DNAUtil.canonicalseq(ukmer);
        Sequence vkmer_canonical = DNAUtil.canonicalseq(vkmer);

        // The canonical direction of the two kmers.
        DNAStrand ukmer_strand;
        DNAStrand vkmer_strand;
        if (ukmer_canonical.equals(ukmer)) {
          ukmer_strand = DNAStrand.FORWARD;
        } else {
          ukmer_strand = DNAStrand.REVERSE;
        }
        if (vkmer_canonical.equals(vkmer)) {
          vkmer_strand = DNAStrand.FORWARD;
        } else {
          vkmer_strand = DNAStrand.REVERSE;
        }

        StrandsForEdge strands = StrandsUtil.form(ukmer_strand, vkmer_strand);
        StrandsForEdge rc_strands = StrandsUtil.complement(strands);

        // Determine the read state for each KMer.
        if (i == 0) {
          if (ukmer_strand == DNAStrand.FORWARD) {
            ustate = ReadState.STARTFORWARD;
          } else {
            ustate = ReadState.STARTREVERSE;
          }
        } else {
          ustate = ReadState.MIDDLE;
        }

        if (i + 1 == end) {
          vstate = ReadState.END;
        } else {
          vstate = ReadState.MIDDLE;
        }

        // TODO(jlewi): It would probably be more efficient not to use a string
        // representation of the Kmers in seen.
        // If the strand and its reverse complement are the same then we want
        // seen to be true because we want to assign the edges from the two
        // strands to different chunk segments.
        boolean seen = (seenmers.contains(ukmer.toString())
            || seenmers.contains(vkmer.toString()) || ukmer.equals(vkmer));
        seenmers.add(ukmer.toString());
        if (seen) {
          // We use the chunk to segment the nodes based on repeat KMers.
          // We use this segmentation at several stages.
          chunk++;
        }

        // Output an edge assuming we are reading the forward strand.
        {
          // TODO(jlewi): Should we verify that all unset bits in node.kmer are
          // 0?
          node.setStrands(strands);
          node.setLastBase(ByteBuffer.wrap(vkmer_end.toPackedBytes(), 0,
              vkmer_end.numPackedBytes()));
          node.setTag(readId);
          node.setState(ustate);
          node.setChunk(chunk);
          outPair.key(ByteBuffer.wrap(ukmer_canonical.toPackedBytes(), 0,
              ukmer_canonical.numPackedBytes()));
          outPair.value(node);
          output.collect(outPair);
        }
        if (seen) {
          chunk++;
        }

        {
          // Output an edge assuming we are reading the reverse strand.
          // TODO(jlewi): Should we verify that all unset bits in node.kmer are
          // 0?
          node.setStrands(rc_strands);
          node.setLastBase(ByteBuffer.wrap(ukmer_start.toPackedBytes(), 0,
              ukmer_start.numPackedBytes()));
          node.setTag(readId);
          node.setState(vstate);
          node.setChunk(chunk);
          outPair.key(ByteBuffer.wrap(vkmer_canonical.toPackedBytes(), 0,
              vkmer_canonical.numPackedBytes()));
          outPair.value(node);
          output.collect(outPair);
        }
        ustate = ReadState.MIDDLE;
      }

      // Add some counters to keep track of how many edges this read produces.
      if (end == 1) {
        reporter.incrCounter("Contrail", "reads-num-edges-1", 1);
      } else if (end>1 && end <= 5) {
        reporter.incrCounter("Contrail", "reads-num-edges-(1,5]", 1);
      } else if (end >5 && end <= 10) {
        reporter.incrCounter("Contrail", "reads-num-edges-(5,10]", 1);
      } else {
        reporter.incrCounter("Contrail", "reads-num-edges-(10,...]", 1);
      }
      reporter.incrCounter("Contrail", "reads-good", 1);
      reporter.incrCounter("Contrail", "reads-goodbp", seq.size());
    }
  }

  /**
   * Reducer for BuildGraph.
   *
   * The reducer outputs a set of key value pairs where the key is a sequence of
   * bytes representing the compressed source KMer. The value is a GraphNodeData
   * datum which contains all the information about edges from the source KMer
   * to different destination KMers.
   *
   * This class is public to facilitate unit-testing.
   */
  public static class BuildGraphReducer extends
      AvroReducer<ByteBuffer, KMerEdge, GraphNodeData> {
    private int K = 0;
    private int MAXTHREADREADS = 0;
    private int MAXR5 = 0;
    private boolean RECORD_ALL_THREADS = false;
    private GraphNode graphnode;
    private Sequence canonical_src;

    @Override
    public void configure(JobConf job) {
      BuildGraphAvro stage = new BuildGraphAvro();
      Map<String, ParameterDefinition> definitions = stage
          .getParameterDefinitions();
      K = (Integer) (definitions.get("K").parseJobConf(job));
      MAXTHREADREADS = (Integer) (definitions.get("MAXTHREADREADS")
          .parseJobConf(job));
      MAXR5 = (Integer) (definitions.get("MAXR5").parseJobConf(job));
      RECORD_ALL_THREADS = (Boolean) (definitions.get("RECORD_ALL_THREADS")
          .parseJobConf(job));

      graphnode = new GraphNode();
      canonical_src = new Sequence(DNAAlphabetFactory.create());
    }

    @Override
    public void reduce(ByteBuffer source_kmer_packed_bytes,
        Iterable<KMerEdge> iterable, AvroCollector<GraphNodeData> collector,
        Reporter reporter) throws IOException {
      Alphabet alphabet = DNAAlphabetFactory.create();

      graphnode.clear();

      canonical_src.readPackedBytes(source_kmer_packed_bytes.array(), K);
      graphnode.setSequence(canonical_src);

      KMerReadTag mertag = null;
      float cov = 0;

      Iterator<KMerEdge> iter = iterable.iterator();
      // Loop over all KMerEdges which originate with the KMer represented by
      // the input key. Since the source and destination KMers overlap for K-1
      // bases, we can construct the destination KMer using the last K-1 bases
      // of the source and the additional base for the destination stored in
      // KMerEdge.
      while (iter.hasNext()) {
        KMerEdge edge = iter.next();

        StrandsForEdge strands = edge.getStrands();

        String read_id = edge.getTag().toString();
        KMerReadTag tag = new KMerReadTag(read_id, edge.getChunk());

        Sequence dest = ConstructDestSequence(canonical_src,
            edge.getLastBase(), strands, alphabet);
        Sequence canonical_dest = DNAUtil.canonicalseq(dest);

        // Set mertag to the smallest (lexicographically) tag
        // of all the tags associated with this key
        if (mertag == null || (tag.compareTo(mertag) < 0)) {
          mertag = tag;
        }

        ReadState state = edge.getState();
        // Update the tags tracking alignment to the start end of a read.
        // We refer to these as R5 tags because it marks the 5 prime end
        // of a read.
        if (state == ReadState.STARTREVERSE) {
          graphnode.addR5(tag, K - 1, DNAStrand.REVERSE, MAXR5);
        } else if (state == ReadState.STARTFORWARD) {
          graphnode.addR5(tag, 0, DNAStrand.FORWARD, MAXR5);
        }

        // Update the coverage.
        if (state == ReadState.MIDDLE) {
          // For KMers in the middle of a read we would generate two
          // edges for the kmer. One corresponding to the kmer and the other
          // corresponding to RC(KMer). Thus, we increment the coverage
          // by .5 so that the result of seeing both edges will be 1.
          cov += .5;
        } else {
          cov += 1;
        }

        // Add an edge to this destination.
        DNAStrand src_strand = StrandsUtil.src(strands);
        String terminalid = constructNodeIdForSequence(canonical_dest);
        EdgeTerminal terminal = new EdgeTerminal(terminalid,
            StrandsUtil.dest(strands));
        graphnode.addOutgoingEdge(src_strand, terminal, tag.toString(),
            MAXTHREADREADS);
      }

      graphnode.setMertag(mertag);
      graphnode.setCoverage(cov);

      // TODO(jlewi): We should at the very least use a compact
      // representation of the sequence.
      graphnode.getData().setNodeId(constructNodeIdForSequence(canonical_src));

      int degree =
          graphnode.degree(DNAStrand.FORWARD, EdgeDirection.INCOMING) +
          graphnode.degree(DNAStrand.FORWARD, EdgeDirection.OUTGOING);

      if (degree == 0) {
        reporter.incrCounter("Contrail", "node-islands", 1);
      }
      collector.collect(graphnode.getData());
      reporter.incrCounter("Contrail", "node-count", 1);
    }
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    ArrayList<InvalidParameter> items = new ArrayList<InvalidParameter>();
    int K = (Integer) stage_options.get("K");

    // K should be odd.
    // If K isn't odd we will get palindromes and we won't preserve important
    // direction information.
    if ((K % 2) == 0) {
      InvalidParameter item = new InvalidParameter(
          "K",
          "K should be odd. If K is even then there exist Kmers for which " +
          "the KMer and its reverse complement are the same. This can cause " +
          "problems for graph construction.");
      items.add(item);
    }
    return items;
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    CompressedRead read = new CompressedRead();
    FastQRecord fastQRecord = new FastQRecord();

    // We need to create a schema representing the union of CompressedRead
    // and FastQRecord because we want to accept either schema for the
    // input.
    schemas.add(read.getSchema());
    schemas.add(fastQRecord.getSchema());
    schemas.add((new Read()).getSchema());
    Schema unionSchema = Schema.createUnion(schemas);

    AvroJob.setInputSchema(conf, unionSchema);
    AvroJob.setMapOutputSchema(conf, BuildGraphAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, BuildGraphAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, BuildGraphMapper.class);
    AvroJob.setReducerClass(conf, BuildGraphReducer.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BuildGraphAvro(), args);
    System.exit(res);
  }
}
