package contrail.stages;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.Sequence;
import contrail.util.ListUtil;

public class TestQuickMergeAvro {
  @Test
  public void testMap() {
    int ntrials = 10;

    for (int trial = 0; trial < ntrials; trial++) {
      GraphNode node = new GraphNode();
      node.setSequence(
          new Sequence("AGTC", DNAAlphabetFactory.create()));
      String readid = "trial_" + trial;
      int chunk = trial*10;
      node.getData().getMertag().setReadTag(readid);
      node.getData().getMertag().setChunk(chunk);

      AvroCollectorMock<Pair<CharSequence, GraphNodeData>> collector_mock =
          new AvroCollectorMock<Pair<CharSequence, GraphNodeData>>();

      ReporterMock reporter_mock = new ReporterMock();
      Reporter reporter = reporter_mock;

      QuickMergeAvro.QuickMergeMapper mapper =
          new QuickMergeAvro.QuickMergeMapper();

      JobConf job = new JobConf(QuickMergeAvro.QuickMergeMapper.class);

      mapper.configure(job);

      try {
        mapper.map(
            node.getData(),
            collector_mock,
            reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }

      // Check the output.
      Iterator<Pair<CharSequence, GraphNodeData>> it =
          collector_mock.data.iterator();
      Pair<CharSequence, GraphNodeData> pair = it.next();

      {
        String expected_key = readid + "_"  + chunk;
        CharSequence output_key = pair.key();
        assertEquals(expected_key, output_key.toString());
      }
      // There should be a single output
      assertFalse(it.hasNext());
    }
  }

  @Test
  public void testReducer() {
    // Construct a sequence of nodes to test the merge for.
    // Construct the graph:
    // ATC->TCG->CGC->GCT->CTA
    // CGC->GCC
    // Which after the merge should produce
    // (id:CGC) ATCGC->GCTA (id:AGC)
    // ATCGC->GCC
    String main_chain = "ATCGCTA";
    int K = 3;
    SimpleGraphBuilder graph = new SimpleGraphBuilder();
    graph.addKMersForString(main_chain, K);

    // Add the edge to break the main chain into two separate chains.
    graph.addEdge("CGC", "GCC", K - 1);

    AvroCollectorMock<GraphNodeData> collector_mock =
        new AvroCollectorMock<GraphNodeData>();

    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    QuickMergeAvro stage = new QuickMergeAvro();
    try {
      List<GraphNodeData> data = new ArrayList<GraphNodeData>();
      for (GraphNode node: graph.getAllNodes().values()) {
        data.add(node.getData());
      }

      QuickMergeAvro.QuickMergeReducer reducer =
          new QuickMergeAvro.QuickMergeReducer();
      JobConf job = new JobConf(QuickMergeAvro.QuickMergeMapper.class);

      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      definitions.get("K").addToJobConf(job, new Integer(3));

      reducer.configure(job);

      reducer.reduce("key",
          data, collector_mock, reporter);
    }
    catch (IOException exception){
      fail("IOException occured in reduce: " + exception.getMessage());
    }

    assertEquals(collector_mock.data.size(), 3);

    // Create a hashtable of the correct sequences.
    Hashtable<String, String> expected_sequences =
        new Hashtable<String, String>();

    // The nodeId's depend on which node we start the merge at,
    // which in turn depends on the ordering of the nodes. This is a bit
    // brittle.
    expected_sequences.put("GCC", "GCC");
    expected_sequences.put("AGC", "GCTA");
    expected_sequences.put("CGC", "ATCGC");

    // Create hashtables for the correct edges.
    Hashtable<String, List<EdgeTerminal>> expected_forward =
          new Hashtable<String, List<EdgeTerminal>>();
    Hashtable<String, List<EdgeTerminal>> expected_reverse =
        new Hashtable<String, List<EdgeTerminal>>();

    {
      List<EdgeTerminal> forward_edges = new ArrayList<EdgeTerminal>();
      forward_edges.add(new EdgeTerminal("AGC", DNAStrand.FORWARD));
      forward_edges.add(new EdgeTerminal("GCC", DNAStrand.FORWARD));
      expected_forward.put("CGC", forward_edges);

      expected_reverse.put("CGC", new ArrayList<EdgeTerminal>());
    }

    {
      List<EdgeTerminal> reverse_edges = new ArrayList<EdgeTerminal>();
      reverse_edges.add(new EdgeTerminal("CGC", DNAStrand.REVERSE));
      expected_reverse.put("AGC", reverse_edges);

      expected_forward.put("AGC", new ArrayList<EdgeTerminal>());
    }

    {
      List<EdgeTerminal> reverse_edges = new ArrayList<EdgeTerminal>();
      reverse_edges.add(new EdgeTerminal("CGC", DNAStrand.REVERSE));
      expected_reverse.put("GCC", reverse_edges);

      expected_forward.put("GCC", new ArrayList<EdgeTerminal>());
    }

    for (Iterator<GraphNodeData> it = collector_mock.data.iterator();
         it.hasNext();) {
      GraphNodeData data = it.next();
      GraphNode node = new GraphNode(data);

      // Check the sequences.
      assertEquals(
          expected_sequences.get(node.getNodeId()),
          node.getSequence().toString());

      // Check the edges.
      List<EdgeTerminal> forward_edges = node.getEdgeTerminals(
          DNAStrand.FORWARD, EdgeDirection.OUTGOING);
      List<EdgeTerminal> reverse_edges = node.getEdgeTerminals(
          DNAStrand.REVERSE, EdgeDirection.OUTGOING);

      List<EdgeTerminal> expected_forward_edges =
          expected_forward.get(node.getNodeId());

      List<EdgeTerminal> expected_reverse_edges =
          expected_reverse.get(node.getNodeId());

      assertTrue(ListUtil.listsAreEqual(expected_forward_edges, forward_edges));
      assertTrue(ListUtil.listsAreEqual(expected_reverse_edges, reverse_edges));
    }
  }
}
