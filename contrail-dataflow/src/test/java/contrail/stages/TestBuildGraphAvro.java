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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.CompressedRead;
import contrail.ContrailConfig;
import contrail.ReadState;
import contrail.ReporterMock;
import contrail.graph.EdgeData;
import contrail.graph.GraphNodeData;
import contrail.graph.KMerEdge;
import contrail.graph.NeighborData;
import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.FastQRecord;
import contrail.sequences.QuakeReadCorrection;
import contrail.sequences.Read;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

public class TestBuildGraphAvro {

  /**
   * Generate a list of all edges coming from the forward read.
   *
   * @param read: String representing the read
   * @param K: Length of the KMers. Generated edges will have length K+1;
   * @param edges: HasMap String -> Int. Edges are added to this hash map The
   *          keys are strings representing the K + 1 length sequences
   *          corresponding to the edges. We consider only edges coming from the
   *          forward direction of the read and not its reverse complement. The
   *          integer represents a count indicating how often that edge
   *          appeared.
   */
  public void allEdgesForForwardRead(String read, int K,
      HashMap<String, java.lang.Integer> edges) {
    for (int i = 0; i <= read.length() - K - 1; i++) {
      String e = read.substring(i, i + K + 1);
      if (!edges.containsKey(e)) {
        edges.put(e, new java.lang.Integer(0));
      }
      edges.put(e, edges.get(e) + 1);
    }
  }

  /**
   * Generate a list of all edges coming from the read and its reverse
   * complement.
   *
   * @param read: String representing the read
   * @return HasMap String -> Int. The keys are strings representing the K+1
   *         sequences corresponding to the edges. We consider both the read and
   *         its reverse complement. The integer represents a count indicating
   *         how often that edge appeared.
   */
  public HashMap<String, java.lang.Integer> allEdgesForRead(String read, int K) {
    HashMap<String, java.lang.Integer> edges = new HashMap<String, java.lang.Integer>();

    allEdgesForForwardRead(read, K, edges);

    Sequence seq = new Sequence(read, DNAAlphabetFactory.create());
    String rc_str = DNAUtil.reverseComplement(seq).toString();
    allEdgesForForwardRead(rc_str, K, edges);
    return edges;
  }

  /**
   * Return a random a random string of the specified length using the given
   * alphabet.
   *
   * @param length
   * @param alphabet
   * @return
   */
  public static String randomString(int length, Alphabet alphabet) {
    // Generate a random sequence of the indicated length;
    char[] letters = new char[length];
    for (int pos = 0; pos < length; pos++) {
      // Randomly select the character in the alpahbet.
      int rnd_int = (int) Math.floor(Math.random() * (alphabet.size() - 1));
      letters[pos] = alphabet.validChars()[rnd_int];
    }
    return String.valueOf(letters);
  }

  /**
   * Helper class which contains the test data for the map phase.
   */
  public static class MapTestData {
    private final CompressedRead read;
    private final int K;
    private final String uncompressed;

    /**
     * Create a specific test case.
     *
     * @param K: Length of the KMer.
     * @param uncompressed: The uncompressed sequence to read.
     */
    public MapTestData(int K, String uncompressed) {
      Alphabet alphabet = DNAAlphabetFactory.create();
      Sequence seq = new Sequence(uncompressed, alphabet);
      read = new CompressedRead();
      read.setDna(ByteBuffer.wrap(seq.toPackedBytes(), 0, seq.numPackedBytes()));
      read.setId("read1");
      read.setLength(seq.size());
      this.K = K;
      this.uncompressed = uncompressed;
    }

    /**
     * Create a random test consisting of a random length read and random value
     * for K.
     *
     * @return An instance of MapTestData cntaining the data for the test.
     */
    public static MapTestData RandomTest() {
      final int MAX_K = 30;
      final int MIN_K = 2;
      final int MAX_LENGTH = 100;
      Alphabet alphabet = DNAAlphabetFactory.create();
      int K = (int) Math.ceil(Math.random() * MAX_K) + 1;
      K = K > MIN_K ? K : MIN_K;
      // Create a compressed read.
      int length = (int) (Math.floor(Math.random() * MAX_LENGTH) + K + 1);
      String uncompressed = randomString(length, alphabet);
      return new MapTestData(K, uncompressed);
    }

    public CompressedRead getRead() {
      return read;
    }

    public String getUncompressed() {
      return uncompressed;
    }

    public int getK() {
      return K;
    }
  }

  private enum ReadInputType {
    COMPRESSED_READ,
    READ,
    FASTQ
  }

  /**
   * Run the test on the mapper.
   *
   * @param useCompressedRead: Whether the input should be a comprssedRead or a
   *          FastQRecord. TODO(jlewi): We should probably test that chunk is
   *          set correctly.
   */
  private void runMapTest(ReadInputType inputType) {
    Alphabet alphabet = DNAAlphabetFactory.create();

    BuildGraphAvro stage = new BuildGraphAvro();
    Map<String, ParameterDefinition> definitions = stage
        .getParameterDefinitions();

    MapTestData test_data = MapTestData.RandomTest();

    AvroCollectorMock<Pair<ByteBuffer, KMerEdge>> collector_mock =
        new AvroCollectorMock<Pair<ByteBuffer, KMerEdge>>();

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    BuildGraphAvro.BuildGraphMapper mapper = new BuildGraphAvro.BuildGraphMapper();

    int K = test_data.getK();
    JobConf job = new JobConf(BuildGraphAvro.BuildGraphMapper.class);
    definitions.get("K").addToJobConf(job, new Integer(test_data.K));
    mapper.configure(job);

    try {
      Object inputRecord = null;
      switch(inputType) {
      case COMPRESSED_READ:
        inputRecord = test_data.getRead();
        break;
      case FASTQ:
      case READ:
        FastQRecord record = new FastQRecord();
        record.setId(test_data.getRead().getId());
        Sequence seq = new Sequence(alphabet);
        seq.readPackedBytes(test_data.getRead().getDna().array(), test_data
            .getRead().getLength());
        record.setRead(seq.toString());
        if (inputType == ReadInputType.FASTQ) {
          inputRecord = record;
        } else {
          Read read = new Read();
          read.setFastq(record);
          read.setQuakeReadCorrection(new QuakeReadCorrection());
          inputRecord = read;
        }
        break;
       default:
         fail("Unreognized input type.");
      }

      mapper.map(inputRecord, collector_mock, reporter);
    } catch (IOException exception) {
      fail("IOException occured in map: " + exception.getMessage());
    }

    // Keep track of the full K + 1 strings corresponding to the
    // edges we read. We keep a count of each edge because it could
    // appear more than once
    HashMap<String, java.lang.Integer> edges = new HashMap<String, java.lang.Integer>();

    // Reconstruct all possible edges coming from the outputs of the mapper.
    for (Iterator<Pair<ByteBuffer, KMerEdge>> it = collector_mock.data
        .iterator(); it.hasNext();) {
      Pair<ByteBuffer, KMerEdge> pair = it.next();

      Sequence canonical_key = new Sequence(DNAAlphabetFactory.create(),
          (int) ContrailConfig.K);
      canonical_key.readPackedBytes(pair.key().array(), test_data.getK());
      Sequence rc_key = DNAUtil.reverseComplement(canonical_key);

      KMerEdge edge = pair.value();

      DNAStrand src_strand = StrandsUtil.src(edge.getStrands());
      DNAStrand dest_strand = StrandsUtil.dest(edge.getStrands());

      // Reconstruct the K+1 string this edge came from.
      Sequence last_base = new Sequence(alphabet);
      last_base.readPackedBytes(edge.getLastBase().array(), 1);

      Sequence edge_seq = new Sequence(alphabet);
      if (src_strand == DNAStrand.FORWARD) {
        edge_seq.add(canonical_key);
      } else {
        edge_seq.add(DNAUtil.reverseComplement(canonical_key));
      }
      edge_seq.add(last_base);

      Sequence dest_kmer = edge_seq.subSequence(1, test_data.getK() + 1);
      // Check that the direction of the destination node is properly encoded.
      // If the sequence and its reverse complement are equal then the link
      // direction could be either forward or reverse because of
      // StransUtil.flip_link.
      if (dest_kmer.equals(DNAUtil.reverseComplement(dest_kmer))) {
        assertTrue(dest_strand == DNAStrand.FORWARD
            || dest_strand == DNAStrand.REVERSE);
      } else {
        assertEquals(DNAUtil.canonicaldir(dest_kmer), dest_strand);
      }

      {
        // Add the edge (K + 1) sequence that would ahve genereated this
        // KMerEdge to the list of edges.
        String edge_str = edge_seq.toString();
        if (!edges.containsKey(edge_str)) {
          edges.put(edge_str, 0);
        }
        edges.put(edge_str, edges.get(edge_str) + 1);
      }

      String uncompressed = test_data.getUncompressed();
      // Check the state as best we can.
      if (edge.getState() == ReadState.STARTFORWARD) {
        assertEquals(src_strand, DNAStrand.FORWARD);
        // The first characters in the string should match this edge.
        assertEquals(canonical_key.toString(), uncompressed.substring(0, K));
      } else if (edge.getState() == ReadState.STARTREVERSE) {
        assertEquals(src_strand, DNAStrand.REVERSE);
        // The first characters in the string should be the reverse complement
        // of the start node.
        assertEquals(rc_key.toString(), uncompressed.substring(0, K));
      } else if (edge.getState() == ReadState.END) {
        // The canonical version of the sequence should match the canonical
        // version of the last Kmer in the read.
        // Get the canonical version of the last K + 1 based in the read.
        Sequence last_kmer = new Sequence(uncompressed.substring(uncompressed
            .length() - K), alphabet);
        last_kmer = DNAUtil.canonicalseq(last_kmer);
        assertEquals(last_kmer, canonical_key);
      }
    }

    // Generate a list of all edges, (substrings of length K + 1), that should
    // be generated by the read.
    HashMap<String, java.lang.Integer> true_edges = allEdgesForRead(
        test_data.getUncompressed(), K);

    // Check that the list of edges from the read matches the set of edges
    // created from the KMerEdges outputted by the mapper.
    assertEquals(true_edges.entrySet(), edges.entrySet());
  }

  // Test the mapper operates correctly when the input is a CompressedRead
  // record.
  @Test
  public void testMapCompressedRead() {
    int ntrials = 10;
    for (int trial = 0; trial < ntrials; trial++) {
      runMapTest(ReadInputType.COMPRESSED_READ);
    }
  }

  // Test the mapper operates correctly when the input is a FastQRecord
  // record.
  @Test
  public void testMapFastQrecord() {
    int ntrials = 10;
    for (int trial = 0; trial < ntrials; trial++) {
      runMapTest(ReadInputType.FASTQ);
    }
  }

  // Test the mapper operates correctly when the input is a Read
  // record.
  @Test
  public void testMapRead() {
    int ntrials = 10;
    for (int trial = 0; trial < ntrials; trial++) {
      runMapTest(ReadInputType.READ);
    }
  }

  /**
   * Used by the testReduce to find a KMerEdge which would have produced an edge
   * between the given source and destination.
   *
   * @param nodeid: The id for the source node. This will be a base64
   *          representation of the canonical sequence.
   * @param K: The length of the KMers.
   * @param neighbor_id: Id for the neighbor.
   * @param strands: Which strands each end of the edge comes from.
   * @param read_tag: The read tag to match. null if you don't want it to be
   *          taken into acount
   * @param Edges: The list of KMerEdges to search to see if it contains one
   *          that could have produced the edge defined by the tuple
   *          (canonical_src, dest_node, link_dir).
   */
  private static boolean foundKMerEdge(String nodeid, int K,
      String neighbor_id, StrandsForEdge strands, String read_tag,
      ArrayList<KMerEdge> edges) {
    Alphabet alphabet = DNAAlphabetFactory.create();

    // Keep track of the positions in edges of the edges that we matched.
    // We will delete these edges so that edges will only contain unmatched
    // eges.
    List<Integer> pos_to_delete = new ArrayList<Integer>();

    Sequence canonical_src = new Sequence(alphabet);
    canonical_src.readBase64(nodeid, K);

    for (int index = 0; index < edges.size(); index++) {
      KMerEdge edge = edges.get(index);

      if (strands != edge.getStrands()) {
        continue;
      }

      // Form the destination sequence for this edge.
      Sequence dest_sequence = BuildGraphAvro.ConstructDestSequence(
          canonical_src, edge.getLastBase(), strands, alphabet);

      Sequence canonical_dest = DNAUtil.canonicalseq(dest_sequence);

      // The ids for the node are the base64 representation of the sequence.
      String kmer_nodeid = canonical_dest.toBase64();

      if (!kmer_nodeid.equals(neighbor_id)) {
        // Destination nodeids don't match.
        continue;
      }

      if (read_tag == null) {
        pos_to_delete.add(index);
      } else {
        if (edge.getTag().toString().equals(read_tag)) {
          pos_to_delete.add(index);
        }
      }
    }

    // Check we found edges that matched.
    assertTrue(pos_to_delete.size() > 0);

    Iterator<Integer> it_pos_to_delete = pos_to_delete.iterator();
    while (it_pos_to_delete.hasNext()) {
      edges.remove(it_pos_to_delete.next().intValue());
    }
    return true;
  }

  /**
   * Class to contain the data for testing the reduce phase.
   */
  public static class ReduceTest {
    public String uncompressed;
    public int K;
    private final List<KMerEdge> input_edges;

    private static Alphabet alphabet = DNAAlphabetFactory.create();

    /**
     * Construct a specific test case.
     *
     * @param uncompressed: The uncompressed K-mer for the source sequence.
     * @param src_strand: Strand of the source KMer.
     * @param last_base: The base we need to add to the soruce KMer to get the
     *          destination KMer.
     */
    public ReduceTest(String uncompressed, DNAStrand src_strand,
        String last_base) {
      this.uncompressed = uncompressed;
      this.K = uncompressed.length();
      input_edges = new ArrayList<KMerEdge>();

      KMerEdge node = new KMerEdge();

      Sequence seq_uncompressed = new Sequence(uncompressed, alphabet);
      Sequence src_canonical = DNAUtil.canonicalseq(seq_uncompressed);
      Sequence seq_last_base = new Sequence(last_base, alphabet);

      // The destination direction depends on the source direction
      // and the K-1 overlap
      Sequence dest_kmer = DNAUtil.sequenceToDir(src_canonical, src_strand);

      dest_kmer = dest_kmer.subSequence(1, dest_kmer.size());
      dest_kmer.add(seq_last_base);

      DNAStrand dest_strand = DNAUtil.canonicaldir(dest_kmer);

      StrandsForEdge strands = StrandsUtil.form(src_strand, dest_strand);
      node.setStrands(strands);

      node.setLastBase(ByteBuffer.wrap(seq_last_base.toPackedBytes(), 0,
          seq_last_base.numPackedBytes()));

      node.setTag("read_0");
      node.setState(ReadState.MIDDLE);
      node.setChunk(0);
      input_edges.add(node);
    }

    /**
     * Construct a reduce test.
     *
     * @param uncompressed: The uncompressed KMer for the source sequence.
     * @param input_edges: A list of KMerEdges describing edges coming from this
     *          KMer.
     * @param K: The length of KMers.
     */
    public ReduceTest(String uncompressed, List<KMerEdge> input_edges, int K) {
      this.uncompressed = uncompressed;
      this.K = K;
      this.input_edges = input_edges;
    }

    public Sequence getSrcSequence() {
      return new Sequence(uncompressed, alphabet);
    }

    public List<KMerEdge> getInputEdges() {
      List<KMerEdge> new_list = new ArrayList<KMerEdge>();
      new_list.addAll(input_edges);
      return new_list;
    }

    /**
     * Generate a random test case.
     */
    public static ReduceTest RandomTest(int MIN_K, int MAX_K, int MAX_EDGES) {
      int K;
      K = (int) Math.ceil(Math.random() * MAX_K) + 1;
      K = K > MIN_K ? K : MIN_K;

      Alphabet alphabet = DNAAlphabetFactory.create();
      // Generate the source KMer.
      List<KMerEdge> input_edges = new ArrayList<KMerEdge>();
      int num_edges = (int) Math.ceil(Math.random() * MAX_EDGES) + 1;

      String uncompressed = randomString(K, alphabet);
      Sequence seq_uncompressed = new Sequence(uncompressed, alphabet);
      Sequence src_canonical = DNAUtil.canonicalseq(seq_uncompressed);

      // Randomly generate the edges.
      for (int eindex = 0; eindex < num_edges; eindex++) {
        KMerEdge node = new KMerEdge();

        // Randomly determine the direction for the source.
        DNAStrand src_strand = DNAStrandUtil.random();

        // Generate the base.
        Sequence last_base = new Sequence(randomString(1, alphabet), alphabet);

        // The destination direction depends on the source direction
        // and the K-1 overlap
        Sequence dest_kmer = DNAUtil.sequenceToDir(src_canonical, src_strand);
        dest_kmer = dest_kmer.subSequence(1, dest_kmer.size());
        dest_kmer.add(last_base);

        DNAStrand dest_strand = DNAUtil.canonicaldir(dest_kmer);
        StrandsForEdge strands = StrandsUtil.form(src_strand, dest_strand);

        node.setStrands(strands);

        node.setLastBase(ByteBuffer.wrap(last_base.toPackedBytes(), 0,
            last_base.numPackedBytes()));
        node.setTag("read_" + eindex);
        // TODO(jlewi): We should pick values for the state and chunk
        // that would make it easy to verify that the reducer properly
        // uses the state and chunk.
        node.setState(ReadState.MIDDLE);
        node.setChunk(0);
        input_edges.add(node);
      }
      return new ReduceTest(uncompressed, input_edges, K);
    }
  }

  // TODO(jlewi): The testing for the reduce phase is quite limited.
  // currently all we check is that the list of edges added is correct.
  // We generate a bunch of edges all with the same source KMer.
  // We then check that the outcome of the reduce phase is a GraphNode
  // with the edges set correctly.
  @Test
  public void TestReduce() {
    final int NTRIALS = 10;
    final int MAX_K = 30;
    final int MIN_K = 2;
    final int MAX_EDGES = 10;

    ReduceTest reduce_data;
    Alphabet alphabet = DNAAlphabetFactory.create();

    BuildGraphAvro stage = new BuildGraphAvro();
    Map<String, ParameterDefinition> definitions = stage
        .getParameterDefinitions();

    for (int trial = 0; trial < NTRIALS; trial++) {
      reduce_data = ReduceTest.RandomTest(MIN_K, MAX_K, MAX_EDGES);

      JobConf job = new JobConf(BuildGraphAvro.BuildGraphReducer.class);

      definitions.get("K").addToJobConf(job, new Integer(reduce_data.K));

      BuildGraphAvro.BuildGraphReducer reducer = new BuildGraphAvro.BuildGraphReducer();
      reducer.configure(job);

      AvroCollectorMock<GraphNodeData> collector_mock = new AvroCollectorMock<GraphNodeData>();

      ReporterMock reporter_mock = new ReporterMock();
      Reporter reporter = reporter_mock;

      Sequence src_canonical = DNAUtil.canonicalseq(reduce_data
          .getSrcSequence());
      try {
        reducer.reduce(ByteBuffer.wrap(src_canonical.toPackedBytes()),
            reduce_data.getInputEdges(), collector_mock, reporter);
      } catch (IOException exception) {
        fail("IOException occured in reduce: " + exception.getMessage());
      }

      // Check the output of the reducer.
      {
        List<KMerEdge> input_edges = reduce_data.getInputEdges();
        assertEquals(collector_mock.data.size(), 1);

        GraphNodeData graph_data = collector_mock.data.get(0);

        // edges_to_find keeps track of all input KMerEdge's which we haven't
        // matched to the output yet. We use this to verify that all KMerEdges
        // are accounted for in the reducer output.
        ArrayList<KMerEdge> edges_to_find = new ArrayList<KMerEdge>();
        edges_to_find.addAll(input_edges);

        // Check the edges in the output node are correct. For each edge in the
        // output we make sure there is a KMerEdge in the input that would have
        // generated that edge. We also count the number of edges to make sure
        // we don't have extra edges. As we go we delete KMerEdges so we don't
        // count any twice.
        for (Iterator<NeighborData> it_dest = graph_data.getNeighbors()
            .iterator(); it_dest.hasNext();) {

          NeighborData dest_node = it_dest.next();

          for (Iterator<EdgeData> it_instances = dest_node.getEdges()
              .iterator(); it_instances.hasNext();) {
            EdgeData edge_data = it_instances.next();

            for (Iterator<CharSequence> it_tag = edge_data.getReadTags()
                .iterator(); it_tag.hasNext();) {
              String tag = it_tag.next().toString();
              // FoundKMerEdge will search for the edge in edges_to_find and
              // remove it if its found.
              assertTrue(foundKMerEdge(graph_data.getNodeId().toString(),
                  reduce_data.K, dest_node.getNodeId().toString(),
                  edge_data.getStrands(), tag, edges_to_find));
            }
          } // for it_instances
        } // for edge_dir
        // Check there were no edges that didn't match.
        assertEquals(edges_to_find.size(), 0);
      }
    } // for trial
  } // TestReduce


  @Test
  public void testCoverage() {
    // Test that the coverage is computed correctly. In particular
    // ensure KMers in the middle of the read aren't computed twice.
    FastQRecord read = new FastQRecord();
    read.setId("read");
    int K = 3;
    read.setRead("ACTGG");
    // Read should be long enough that there is an internal KMer.
    // Otherwise the test is insufficient.
    assertTrue(read.getRead().length() >= K +2);

    BuildGraphAvro stage = new BuildGraphAvro();

    JobConf job = new JobConf(BuildGraphAvro.BuildGraphMapper.class);
    stage.getParameterDefinitions().get("K").addToJobConf(job, new Integer(K));
    BuildGraphAvro.BuildGraphMapper mapper =
        new BuildGraphAvro.BuildGraphMapper();
    mapper.configure(job);

    AvroCollectorMock<Pair<ByteBuffer, KMerEdge>> collector =
        new AvroCollectorMock<Pair<ByteBuffer, KMerEdge>>();
    ReporterMock reporter = new ReporterMock();

    try {
      mapper.map(read, collector, reporter);
    } catch (Exception e) {
      fail("Mapper failed.");
    }

    // Group the mapper outputs.
    HashMap<String, ArrayList<KMerEdge>> reduceGroups =
        new HashMap<String, ArrayList<KMerEdge>>();
    for (Pair<ByteBuffer, KMerEdge> pair : collector.data) {
      Sequence sequence = new Sequence(DNAAlphabetFactory.create());
      sequence.readPackedBytes(pair.key().array(), K);
      String key = sequence.toString();
      if (!reduceGroups.containsKey(key)) {
        reduceGroups.put(key, new ArrayList<KMerEdge>());
      }
      reduceGroups.get(key).add(pair.value());
    }

    // Invoke the reducer.
    BuildGraphAvro.BuildGraphReducer reducer =
        new BuildGraphAvro.BuildGraphReducer();
    reducer.configure(job);

    AvroCollectorMock<GraphNodeData> reduceCollector =
        new AvroCollectorMock<GraphNodeData>();
    try {
      for (String key : reduceGroups.keySet()) {
        Sequence sequence = new Sequence(key, DNAAlphabetFactory.create());
        ByteBuffer buffer = ByteBuffer.wrap(sequence.toPackedBytes());

        try {
          reducer.reduce(
              buffer, reduceGroups.get(key), reduceCollector, reporter);
        } catch (Exception e) {
          fail("Mapper failed.");
        }
      }
    } catch (Exception e) {
      fail("Reducer failed.");
    }

    // Check the output.
    HashSet<String> nodeIds = new HashSet<String>();
    for (GraphNodeData node : reduceCollector.data) {
      assertEquals(1.0f, node.getCoverage(), 0.00001);
      nodeIds.add(node.getNodeId().toString());
    }

    assertEquals(3, nodeIds.size());
  }
}
