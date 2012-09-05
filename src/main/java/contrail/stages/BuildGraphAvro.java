package contrail.stages;
import contrail.CompressedRead;
import contrail.ReadState;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.KMerEdge;
import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.KMerReadTag;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.Schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class BuildGraphAvro extends Stage {
  private static final Logger sLogger = Logger.getLogger(BuildGraphAvro.class);

  public static final Schema kmer_edge_schema = (new KMerEdge()).getSchema();
  public static final Schema graph_node_data_schema =
      (new GraphNodeData()).getSchema();

  /**
   * Define the schema for the mapper output. The keys will be a byte buffer
   * representing the compressed source KMer sequence. The value will be an
   * instance of KMerEdge.
   */
  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(Schema.create(Schema.Type.BYTES), kmer_edge_schema);

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
   * @param sequence
   * @return
   */
  public static String constructNodeIdForSequence(Sequence sequence) {
    // TODO(jlewi): We should at the very least use a compact
    // representation of the sequence.
    return sequence.toString();
  }

  protected Map<String, ParameterDefinition>
    createParameterDefinitions() {
      HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    // Add options specific to this stage.
    ParameterDefinition max_reads =
        new ParameterDefinition("max_reads",
            "max reads starts per node.", Integer.class, new Integer(250));

    ParameterDefinition trim3 =
        new ParameterDefinition("TRIM3",
            "Chopped bases.", Integer.class, new Integer(0));

    ParameterDefinition trim5 =
        new ParameterDefinition("TRIM5",
            "Chopped bases.", Integer.class, new Integer(0));

    ParameterDefinition maxR5 =
        new ParameterDefinition("MAXR5",
            "Max R5.", Integer.class, new Integer(250));


    ParameterDefinition max_thread_reads =
        new ParameterDefinition("MAXTHREADREADS",
            "Max thread reads.", Integer.class, new Integer(250));


    ParameterDefinition record_all_threads =
        new ParameterDefinition("RECORD_ALL_THREADS",
            "Record all threads.", Boolean.class, new Boolean(false));

    for (ParameterDefinition def:
      new ParameterDefinition[] {
        max_reads, trim3, trim5, maxR5, max_thread_reads, record_all_threads}) {
      defs.put(def.getName(), def);
    }

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Construct the destination KMer in an edge using the source KMer,
   * the last base for the sequence, and the strands for the edge.
   *
   * The mapper does a micro optimization. Since, the two KMers overlap by
   * K-1 bases we can construct the destination KMer from the
   * source KMer and the non-overlap region of the destination
   *
   * @param canonical_src: Canonical representation of the source KMer.
   * @param last_base: The non overlap region of the destination KMer.
   * @param strands: Which strands the source and destination kmer came from.
   * @param alphabet: The alphabet for the encoding.
   * @return: The destination KMer.
   */
  public static Sequence ConstructDestSequence(
      Sequence canonical_src, ByteBuffer last_base_byte, StrandsForEdge strands,
      Alphabet alphabet) {
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
   * This object is instantiated once for each mapper and customizes the operations
   * based on the settings and the alphabet.
   *
   * We use a separate class so that its easy to unittest.
   */
  public static class SequencePreProcessor {

    private Alphabet alphabet;
    private int trim5;
    private int trim3;

    private boolean hasTrimVal;
    private int trimVal;

    /**
     * @param alphabet - The alphabet.
     * @param trim5 - Number of bases to trim off the start
     * @param trim3 - Number of bases to trim off the end.
     */
    public SequencePreProcessor(Alphabet alphabet, int trim5, int trim3) {
      this.alphabet = alphabet;
      this.trim5 = trim5;
      this.trim3 = trim3;

      // Check if this alphabet has a character which should be removed from
      // both ends of the sequence.
      if (alphabet.hasLetter('N')) {
        hasTrimVal = true;
        trimVal = alphabet.letterToInt('N');
      }
      else {
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
   * Mapper for BuildGraph.
   * Class is public to facilitate unit-testing.
   */
  public static class BuildGraphMapper extends
  AvroMapper<CompressedRead, Pair<ByteBuffer, KMerEdge>>
  {
    private static int K = 0;

    private Alphabet alphabet = DNAAlphabetFactory.create();
    private Sequence seq = new Sequence(DNAAlphabetFactory.create());
    private SequencePreProcessor preprocessor;

    private KMerEdge node = new KMerEdge();
    public void configure(JobConf job)
    {
      BuildGraphAvro stage = new BuildGraphAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));
      if (K <= 0) {
        throw new RuntimeException("K must be a positive integer");
      }
      int TRIM5 = (Integer)(definitions.get("TRIM5").parseJobConf(job));
      int TRIM3 = (Integer)(definitions.get("TRIM3").parseJobConf(job));;

      preprocessor = new SequencePreProcessor(alphabet, TRIM5, TRIM3);
    }


    /*
     * Input (CompressedRead) - Each input is an instance of CompressedRead.
     *
     * Output (ByteBuffer, KMerEdge): The output key is a sequence of bytes
     *   representing the compressed KMer for the source node. The value
     *   is an instance of KMerEdge which contains all the information
     *   for an edge originating from this source KMer.
     *
     * For each successive pair of k-mers in the read, we output two
     * tuples; where each tuple corresponds to the read coming from a different
     * strand of the sequence.
     */
    @Override
    public void map(CompressedRead compressed_read,
        AvroCollector<Pair<ByteBuffer, KMerEdge>> output, Reporter reporter)
            throws IOException {

      seq.readPackedBytes(compressed_read.getDna().array(),
          compressed_read.getLength());
      seq = preprocessor.PreProcess(seq);

      // Check for short reads.
      if (seq.size() <= K)
      {
        reporter.incrCounter("Contrail", "reads_short", 1);
        return;
      }

      ReadState ustate = ReadState.END5;
      ReadState vstate = ReadState.I;

      Set<String> seenmers = new HashSet<String>();

      int chunk = 0;

      int end = seq.size() - K;

      // Now emit the edges of the de Bruijn Graph.
      // Each successive kmer in the read is a node in the graph
      // and the edge connecting them is the (k-1) of overlap.
      // Since we don't know which strand the read came from, we need
      // to consider both the read and its reverse complement when generating
      // edges.
      for (int i = 0; i < end; i++)
      {
        // ukmer and vkmer are sequential KMers in the read.
        Sequence ukmer = seq.subSequence(i, i+K);
        Sequence vkmer = seq.subSequence(i+1, i+1+K);

        // ukmer_start and vkmer_end are the base we need to add
        // to the source kmer in order to generate the destination KMer.
        Sequence ukmer_start = seq.subSequence(i, i+1);
        Sequence vkmer_end = seq.subSequence(i+K, i+K+1);
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

        if ((i == 0) && (ukmer_strand == DNAStrand.REVERSE))  { ustate = ReadState.END6; }
        if (i+1 == end) { vstate = ReadState.END3; }

        // TODO(jlewi): It would probably be more efficient not to use a string
        // representation of the Kmers in seen.
        // If the strand and its reverse complement are the same then we want
        // seen to be true because we want to assign the edges from the two
        // strands to different chunk segments.
        boolean seen = (seenmers.contains(ukmer.toString()) ||
            seenmers.contains(vkmer.toString()) ||
            ukmer.equals(vkmer));
        seenmers.add(ukmer.toString());
        if (seen) {
          // We use the chunk to segment the nodes based on repeat KMers.
          // We use this segmentation at several stages.
          chunk++;
        }

        // Output an edge assuming we are reading the forward strand.
        {
          Pair<ByteBuffer, KMerEdge> pair =
              new Pair<ByteBuffer, KMerEdge>(MAP_OUT_SCHEMA);
          ByteBuffer key;

          // TODO(jlewi): Should we verify that all unset bits in node.kmer are
          // 0?
          node.setStrands(strands);
          node.setLastBase(
              ByteBuffer.wrap(vkmer_end.toPackedBytes(), 0,
                              vkmer_end.numPackedBytes()));
          node.setTag(compressed_read.getId());
          node.setState(ustate);
          node.setChunk(chunk);
          key = ByteBuffer.wrap(ukmer_canonical.toPackedBytes(), 0,
              ukmer_canonical.numPackedBytes());
          pair.set(key, node);
          output.collect(pair);
        }
        if (seen)
        {
          chunk++;
        }

        {
          Pair<ByteBuffer, KMerEdge> pair =
              new Pair<ByteBuffer, KMerEdge>(MAP_OUT_SCHEMA);
          ByteBuffer key;
          // Output an edge assuming we are reading the reverse strand.
          // TODO(jlewi): Should we verify that all unset bits in node.kmer are
          // 0?
          node.setStrands(rc_strands);
          node.setLastBase(ByteBuffer.wrap(ukmer_start.toPackedBytes(), 0, ukmer_start.numPackedBytes()));
          node.setTag(compressed_read.id);
          node.setState(vstate);
          node.setChunk(chunk);
          key = ByteBuffer.wrap(vkmer_canonical.toPackedBytes(), 0,
              vkmer_canonical.numPackedBytes());
          pair.set(key, node);
          output.collect(pair);
        }
        ustate = ReadState.MIDDLE;
      }
      reporter.incrCounter("Contrail", "reads_good", 1);
      reporter.incrCounter("Contrail", "reads_goodbp", seq.size());
    }
  }

  /**
   * Reducer for BuildGraph.
   *
   * The reducer outputs a set of key value pairs where the key is a sequence
   * of bytes representing the compressed source KMer. The value is a
   * GraphNodeData datum which contains all the information about edges from
   * the source KMer to different destination KMers.
   *
   * This class is public to facilitate unit-testing.
   */
  public static class BuildGraphReducer extends
      AvroReducer<ByteBuffer, KMerEdge, GraphNodeData> {
    private static int K = 0;
    private static int MAXTHREADREADS = 0;
    private static int MAXR5 = 0;
    private static boolean RECORD_ALL_THREADS = false;

    public void configure(JobConf job) {
      BuildGraphAvro stage = new BuildGraphAvro();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));
      MAXTHREADREADS = (Integer)
          (definitions.get("MAXTHREADREADS").parseJobConf(job));
      MAXR5 = (Integer) (definitions.get("MAXR5").parseJobConf(job));
      RECORD_ALL_THREADS = (Boolean)
          (definitions.get("RECORD_ALL_THREADS").parseJobConf(job));
    }

    @Override
    public void reduce(ByteBuffer source_kmer_packed_bytes, Iterable<KMerEdge> iterable,
        AvroCollector<GraphNodeData> collector, Reporter reporter)
            throws IOException {
      Alphabet alphabet = DNAAlphabetFactory.create();
      GraphNode graphnode = new GraphNode();
      graphnode.setSequence(source_kmer_packed_bytes, K);

      Sequence canonical_src = new Sequence(alphabet);
      canonical_src.readPackedBytes(source_kmer_packed_bytes.array(), K);

      KMerReadTag mertag = null;
      int cov = 0;

      Iterator<KMerEdge> iter = iterable.iterator();
      // Loop over all KMerEdges which originate with the KMer represented by
      // the input key. Since the source and destination KMers overlap for K-1
      // bases, we can construct the destination KMer using the last K-1 bases
      // of the source and the additional base for the destination stored in
      // KMerEdge.
      while(iter.hasNext()) {
        KMerEdge edge = iter.next();

        StrandsForEdge strands = edge.getStrands();

        String read_id = edge.getTag().toString();
        KMerReadTag tag = new KMerReadTag(read_id, edge.getChunk());

        Sequence dest = ConstructDestSequence(
            canonical_src, edge.getLastBase(), strands, alphabet);
        Sequence canonical_dest = DNAUtil.canonicalseq(dest);

        // Set mertag to the smallest (lexicographically) tag
        // of all the tags associated with this key
        if (mertag == null || (tag.compareTo(mertag) < 0)) {
          mertag = tag;
        }

        ReadState state = edge.getState();
        // Update coverage and offsets.
        if (state != ReadState.I) {
          cov++;
          if (state == ReadState.END6) {
            graphnode.addR5(tag, K-1, DNAStrand.REVERSE, MAXR5);
          } else if (state == ReadState.END5) {
            graphnode.addR5(tag, 0, DNAStrand.FORWARD, MAXR5);
          }
        }
        // Add an edge to this destination.
        DNAStrand src_strand = StrandsUtil.src(strands);
        String terminalid = constructNodeIdForSequence(canonical_dest);
        EdgeTerminal terminal = new EdgeTerminal(
            terminalid, StrandsUtil.dest(strands));
        graphnode.addOutgoingEdge(src_strand, terminal, tag.toString(),
            MAXTHREADREADS);
      }

      graphnode.setMertag(mertag);
      graphnode.setCoverage(cov);

      // TODO(jlewi): We should at the very least use a compact
      // representation of the sequence.
      graphnode.getData().setNodeId(
          constructNodeIdForSequence(canonical_src));

      collector.collect(graphnode.getData());
      reporter.incrCounter("Contrail", "nodecount", 1);
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    // Check for missing arguments.
    String[] required_args = {"inputpath", "outputpath", "K"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    int K = (Integer)stage_options.get("K");

    // K should be odd.
    if ((K % 2) == 0) {
      throw new RuntimeException (
          "K should be odd. If K is even then there exist Kmers for which " +
          "the KMer and its reverse complement are the same. This can cause " +
          "problems for graph construction.");
    }
    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }
    conf.setJobName("BuildGraph " + inputPath + " " + K);

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    CompressedRead read = new CompressedRead();
    AvroJob.setInputSchema(conf, read.getSchema());
    AvroJob.setMapOutputSchema(conf, BuildGraphAvro.MAP_OUT_SCHEMA);
    AvroJob.setOutputSchema(conf, BuildGraphAvro.REDUCE_OUT_SCHEMA);

    AvroJob.setMapperClass(conf, BuildGraphMapper.class);
    AvroJob.setReducerClass(conf, BuildGraphReducer.class);

    if (stage_options.containsKey("writeconfig")) {
      writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already
      Path out_path = new Path(outputPath);
      if (FileSystem.get(conf).exists(out_path)) {
        // TODO(jlewi): We should only delete an existing directory
        // if explicitly told to do so.
        sLogger.info("Deleting output path: " + out_path.toString() + " " +
            "because it already exists.");
        FileSystem.get(conf).delete(out_path, true);
      }

      long starttime = System.currentTimeMillis();
      RunningJob result = JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);

      sLogger.info("Runtime: " + diff + " s");
      return result;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BuildGraphAvro(), args);
    System.exit(res);
  }
}
