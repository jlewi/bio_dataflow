// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.graph;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.Node;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;
import contrail.util.ListUtil;

public class TestNodeConverter {

  // Random number generator.
  private Random generator;
  @Before
  public void setUp() {
    // Create a random generator so we can make a test repeatable
    // generator = new Random(103);
    generator = new Random();
  }

  protected GraphNode randomGraphNode() {
    GraphNode graph_node = new GraphNode();
    {
      String sequence_str = AlphabetUtil.randomString(
          generator, generator.nextInt(100) + 3, DNAAlphabetFactory.create());
      Sequence canonical_sequence;
      canonical_sequence = new Sequence(
          sequence_str, DNAAlphabetFactory.create());
      canonical_sequence = DNAUtil.canonicalseq(canonical_sequence);
      graph_node.setSequence(canonical_sequence);
    }

    graph_node.setNodeId("trial_node_" + generator.nextInt(100));

    {
      GraphNodeKMerTag tag = new GraphNodeKMerTag();
      tag.setReadTag("read_tag_" + generator.nextInt(123));
      tag.setChunk(generator.nextInt(100));
      graph_node.getData().setMertag(tag);
    }

    graph_node.setCoverage(generator.nextFloat() * 100);

    // Add some edges.
    int num_edges = generator.nextInt(10) + 2;
    for (int edge_index = 0; edge_index < num_edges; ++edge_index) {
      DNAStrand src_strand = DNAStrandUtil.random(generator);
      DNAStrand dest_strand = DNAStrandUtil.random(generator);
      EdgeTerminal dest = new EdgeTerminal("dest_" + edge_index, dest_strand);

      List<CharSequence> tags = new ArrayList<CharSequence>();

      int num_tags = generator.nextInt(7);

      for (int tag_index = 0; tag_index < num_tags; ++tag_index) {
        tags.add("tag_" + tag_index);
      }
      graph_node.addOutgoingEdgeWithTags(
          src_strand, dest, tags, java.lang.Integer.MAX_VALUE);
    }

    // Add R5Tags.
    int num_r5tags = generator.nextInt(5);
    for (int index = 0; index < num_r5tags; ++index) {
      R5Tag tag = new R5Tag();
      tag.setTag("r5tag" + index);
      tag.setOffset(generator.nextInt(100));
      tag.setStrand(DNAStrandUtil.random(generator));
      graph_node.getData().getR5Tags().add(tag);
    }

    return graph_node;
  }
  /**
   * Parse the list of strings returned by Node.getThreads().
   * Each string in the list has the form
   * StrandsForEdge.toString(): Destination Id: read id.
   * We return this as a Map: Destination Id - > (Map: Strands-> readIds)
   */
  protected Hashtable<String, Hashtable<StrandsForEdge, List<String>>>
      parseThreads(List<String> tags) {
    Hashtable<String, Hashtable<StrandsForEdge, List<String>>> node_to_strands
        = new  Hashtable<String, Hashtable<StrandsForEdge, List<String>>> ();
    for (String tag: tags) {
      String[] parts = tag.split(":");
      StrandsForEdge strands = StrandsUtil.parse(parts[0]);
      String destid = parts[1];
      String read = parts[2];

      Hashtable<StrandsForEdge, List<String>> tags_for_node =
          node_to_strands.get(destid);
      if (tags_for_node == null) {
        tags_for_node = new Hashtable<StrandsForEdge, List<String>> ();
        node_to_strands.put(destid, tags_for_node);
      }

      List<String> read_tags = tags_for_node.get(strands);
      if (read_tags == null) {
        read_tags = new ArrayList<String> ();
        tags_for_node.put(strands, read_tags);
      }
      read_tags.add(read);
    }

    return node_to_strands;
  }

  protected List<String> charListToStringList(List<CharSequence> in_list) {
    List<String> out_list = new ArrayList<String>();
    for (CharSequence item: in_list) {
      out_list.add(item.toString());
    }
    return out_list;
  }
  /**
   * Ensure the node equals the graph_node.
   * @param graph_node
   * @param node
   */
  protected void checkEquals(GraphNode graph_node, Node node) {
    assertEquals(graph_node.getNodeId(), node.getNodeId());
    assertEquals(graph_node.getCoverage(), node.cov(), .001);

    assertEquals(graph_node.getSequence().toString(), node.str());
    {
      String expected_mertag =
          graph_node.getData().getMertag().getReadTag().toString() + "_" +
          graph_node.getData().getMertag().getChunk();

      String mertag = "";
      try {
        mertag = node.getMertag();
      } catch (IOException exception) {
        fail("An exception occured trying to get the mertag");
      }

      assertEquals(expected_mertag, mertag);
    }

    // Compare the edges.
    for (StrandsForEdge strands: StrandsForEdge.values()) {
      List<String> neighbor_ids = null;
      try {
        neighbor_ids = node.getEdges(
            strands.toString().toLowerCase());
      } catch (IOException exception) {
        fail("An exception occured trying to get the edges");
      }

      List<String> expected_ids = new ArrayList<String>();
      {
        List<CharSequence> ids = graph_node.getNeighborsForStrands(strands);
        for (CharSequence id: ids) {
          expected_ids.add(id.toString());
        }
      }

      if (expected_ids.size() > 0) {
        ListUtil.listsAreEqual(expected_ids, neighbor_ids);
      } else {
        assertEquals(null, neighbor_ids);
      }

    }

    // Compare the read tags associated with the edges.
    Hashtable<String, Hashtable<StrandsForEdge, List<String>>>  tags =
        parseThreads(node.getThreads());

    for (DNAStrand strand: DNAStrand.values()) {
      for (EdgeTerminal terminal: graph_node.getEdgeTerminals(
          strand, EdgeDirection.OUTGOING)) {
        List<String> expected_tags = charListToStringList(
            graph_node.getTagsForEdge(strand, terminal));

        StrandsForEdge strands = StrandsUtil.form(strand, terminal.strand);

        if (!tags.containsKey(terminal.nodeId)) {
          assertEquals(0, expected_tags.size());
        } else {
          List<String> node_tags = tags.get(terminal.nodeId).get(strands);
          if (expected_tags.size() == 0) {
            assertEquals(null, node_tags);
          } else {
            assertTrue(ListUtil.listsAreEqual(expected_tags, node_tags));
          }
        }
      }
    }

    List<String> r5_tags = node.getreads();
    List<String> expected_r5_tags = new ArrayList<String>();
    {
      List<R5Tag> object_tags = graph_node.getData().getR5Tags();
      for(R5Tag tag: object_tags) {
        String tag_str = tag.getTag().toString() + ":" + tag.getOffset();

        if (tag.getStrand() == DNAStrand.REVERSE) {
          tag_str = "~" + tag_str;
        }
        expected_r5_tags.add(tag_str);
      }
    }
    if (expected_r5_tags.size() == 0) {
      assertEquals(null, r5_tags);
    } else {
      assertTrue(ListUtil.listsAreEqual(expected_r5_tags, r5_tags));
    }
  }

  @Test
  public void testGraphNodeToNode() {
    // Test the conversion from the new avro format to the old
    // format.
    int num_trials = 10;

    for (int trial = 0; trial < num_trials; ++trial) {
      GraphNode graph_node = randomGraphNode();
      Node node = NodeConverter.graphNodeToNode(graph_node);
      checkEquals(graph_node, node);
    }
  }
}
