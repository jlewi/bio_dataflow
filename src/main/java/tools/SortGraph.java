package tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
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

import contrail.CompressedRead;
import contrail.ReadState;
import contrail.avro.BuildGraphAvro;
import contrail.avro.ContrailParameters;
import contrail.avro.ParameterDefinition;
import contrail.avro.BuildGraphAvro.BuildGraphMapper;
import contrail.avro.BuildGraphAvro.BuildGraphReducer;
import contrail.avro.BuildGraphAvro.SequencePreProcessor;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.KMerEdge;
import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAUtil;
import contrail.sequences.KMerReadTag;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

/**
 * A simple mapreduce job which sorts the graph by the node ids.
 */
public class SortGraph {
  private static final Logger sLogger = Logger.getLogger(SortGraph.class);


  public static final Schema graph_node_data_schema =
      (new GraphNodeData()).getSchema();
  /**
   * Define the schema for the mapper output. The keys will be a byte buffer
   * representing the compressed source KMer sequence. The value will be an
   * instance of KMerEdge.
   */
  public static final Schema MAP_OUT_SCHEMA =
      Pair.getPairSchema(
          Schema.create(Schema.Type.STRING), graph_node_data_schema);

  /**
   * Define the schema for the reducer output.
   */
  public static final Schema REDUCE_OUT_SCHEMA = graph_node_data_schema;

  public static class BuildGraphMapper extends
  AvroMapper<GraphNodeData, Pair<CharSequence, GraphNodeData>>
  {
    private Pair<CharSequence, GraphNodeData> pair;
    public void configure(JobConf job)
    {
      pair = new Pair<CharSequence, GraphNodeData>();
    }

    @Override
    public void map(GraphNodeData input,
        AvroCollector<Pair<CharSequence, GraphNodeData>> output, Reporter reporter)
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
    int res = ToolRunner.run(new Configuration(), new SortGraph(), args);
    System.exit(res);
  }
}
