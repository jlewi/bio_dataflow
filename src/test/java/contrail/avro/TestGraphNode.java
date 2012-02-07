package contrail.avro;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import contrail.DestForLinkDir;
import contrail.EdgeDestNode;
import contrail.GraphNodeData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TestGraphNode {

  /**
   * Return the list of possible link direction in random permutation order
   * @return
   */
  protected String[] permuteLinkDirs() {
    List<String> link_dirs = new ArrayList<String>();
    link_dirs.add("ff");
    link_dirs.add("fr");
    link_dirs.add("rf");
    link_dirs.add("rr");
    
//    List<String> permutation = new ArrayList<String>();
//    while (link_dirs.size() > 0) {
//      // Randomly pick one of the items in link_ders.
//      int pos = (int) Math.floor(Math.random() * link_dirs.size());
//      permutation.add(link_dirs.get(pos));
//      link_dirs.remove(pos);
//    }
    String[] return_type = new String[]{};
    Collections.shuffle(link_dirs);
    return link_dirs.toArray(return_type);
  }
  

  public static class CharSequenceComparator implements Comparator {
    public int compare (Object o1, Object o2) {
      return o1.toString().compareTo(o2.toString());
    }
    public boolean equals(Object obj) {
      return obj instanceof CharSequenceComparator;
    }
  }
  @Test
  public void testGetDestIdsForSrcDir() {
    // Create a graph node with some edges and verify getDestIdsForSrcDir
    // returns the correct data.
    GraphNodeData node_data = new GraphNodeData();
    node_data.setNodeId("node");
    node_data.setDestNodes(new ArrayList<EdgeDestNode> ());
    int num_edges = (int) Math.floor(Math.random() * 100) + 1;
    
    HashMap<String, List<String>> true_nodes_for_link_dirs =
        new HashMap<String, List<String>> ();
    
    for (int index = 0; index < num_edges; index++) {
      EdgeDestNode edge_dest = new EdgeDestNode();
      node_data.getDestNodes().add(edge_dest);
      edge_dest.setNodeId("edge_" + index);
      
      //List<DestForLink>dests_for_links = new ArrayList<DestForLinkDir>()
      edge_dest.setLinkDirs(new ArrayList<DestForLinkDir>());
      
      int num_links = (int) Math.floor(Math.random() * 4) + 1;
      String[] link_dirs = permuteLinkDirs();
      for (int link_index = 0; link_index < num_links; link_index++) {
        String dir = link_dirs[link_index];
        DestForLinkDir dest_for_link = new DestForLinkDir();
        dest_for_link.setLinkDir(dir);
        edge_dest.getLinkDirs().add(dest_for_link);
        
         
        // Add this node to true_nodes_for_link_dirs;
        if (!true_nodes_for_link_dirs.containsKey(dir)) {
          true_nodes_for_link_dirs.put(dir, new ArrayList<String>());
        }
        true_nodes_for_link_dirs.get(dir).add(
            edge_dest.getNodeId().toString());
      }
    }

    //********************************************************************
    // Run the test.
    // Create a GraphNode.
    GraphNode node = new GraphNode();
    node.setData(node_data);
    String[] all_link_dirs = new String[] {"ff", "fr", "rf", "rr"};
    for (String dir: all_link_dirs) {
      if (!true_nodes_for_link_dirs.containsKey(dir)) {
        assertEquals(null, node.getDestIdsForSrcDir(dir));
      } else {
        List<CharSequence> dest_ids = node.getDestIdsForSrcDir(dir);
        //List<String> dest_ids = new ArrayList<String> ();
        //dest_ids.addAll(dest_ids_char);
        Collections.sort(dest_ids, new CharSequenceComparator());
        
        List<String> true_dest_ids = true_nodes_for_link_dirs.get(dir);
        Collections.sort(true_dest_ids);
        
        assertTrue(true_dest_ids.equals(dest_ids));
      }
    }
  }
}
