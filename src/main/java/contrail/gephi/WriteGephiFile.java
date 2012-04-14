package contrail.gephi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAStrand;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Write a gephi XML file to represent a graph.
 * 
 * WARNING: Gephi appears to have problems reading files in "/tmp" so
 * write the file somewhere else.
 */
public class WriteGephiFile {  
  private static final Logger sLogger = 
      Logger.getLogger(WriteGephiFile.class);
  
  /**
   * Create an XML element to represent the edge.
   * @param edge_id
   * @param node_id_map
   * @param node
   * @param terminal
   * @return
   */
  private static Element createElementForEdge(      
      Document doc, int edge_id, HashMap<String, Integer> node_id_map,
      GraphNode node, EdgeTerminal terminal) {
    Element xml_edge = doc.createElement("edge");
    Integer this_node_id = node_id_map.get(node.getNodeId());
    
    xml_edge.setAttribute("id", Integer.toString(edge_id));
    edge_id++;
    
    xml_edge.setAttribute("source", this_node_id.toString());        
    Integer target_id = node_id_map.get(terminal.nodeId);
    xml_edge.setAttribute("target", target_id.toString());
    return xml_edge; 
  }
  
  public static void writeGraph(List<GraphNode> nodes, String xml_file) {
    if (xml_file.startsWith("/tmp")) {
      throw new RuntimeException(
          "Don't write the file to '/tmp' gephi has problems with that.");
    }
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = null;
    try {
       dBuilder = dbFactory.newDocumentBuilder();
    } catch (Exception exception) {
        sLogger.error("Exception:" + exception.toString());
    }
    Document doc = dBuilder.newDocument();

    Element gexf_root = doc.createElement("gexf");
    doc.appendChild(gexf_root);
    Element root = doc.createElement("graph");
    gexf_root.appendChild(root);
    root.setAttribute("mode", "static");
    root.setAttribute("defaultedgetype", "directed");
    
    
    Element xml_nodes = doc.createElement("nodes");
    Element xml_edges = doc.createElement("edges");
    
    root.appendChild(xml_nodes);
    root.appendChild(xml_edges);
    
    // We assign each edge a unique id.
    int edge_id = 0;
    
    // I think the id's in the gephi xml file need to be string representations
    // of integers so we assign each node an integer.
    HashMap<String, Integer> node_id_map = new HashMap<String, Integer>();
    
    // Create a lookup table so we can map nodeId's to their integers.
    for (int index = 0; index < nodes.size(); ++index) {
      node_id_map.put(nodes.get(index).getNodeId(), index);
    }
    
    for (GraphNode node: nodes) {
      Element xml_node = doc.createElement("node");
      xml_nodes.appendChild(xml_node);
      
      Integer this_node_id = node_id_map.get(node.getNodeId());
          
      xml_node.setAttribute("id", this_node_id.toString());
      xml_node.setAttribute("label", node.getNodeId());
      xml_nodes.appendChild(xml_node);
            
      List<EdgeTerminal> forward_edges =
          node.getEdgeTerminals(DNAStrand.FORWARD, EdgeDirection.OUTGOING);
            
      for (EdgeTerminal terminal: forward_edges){
        Element xml_edge = createElementForEdge(      
            doc, ++edge_id, node_id_map, node, terminal);
        xml_edges.appendChild(xml_edge);
      }
      
      List<EdgeTerminal> reverse_edges =
          node.getEdgeTerminals(DNAStrand.REVERSE, EdgeDirection.OUTGOING);
            
      for (EdgeTerminal terminal: reverse_edges){
        Element xml_edge = createElementForEdge(      
            doc, ++edge_id, node_id_map, node, terminal);
        xml_edges.appendChild(xml_edge);
      }
      
      // write the content into xml file
      try {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(xml_file);
        transformer.transform(source, result);
      } catch (Exception exception) {
        sLogger.error("Exception:" + exception.toString());
      }
    }    
  }
  
  public static void main(String[] args) {
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTCGAT", 3);
    
    List<GraphNode> nodes = new ArrayList<GraphNode>();
    nodes.addAll(builder.getAllNodes().values());
    writeGraph(nodes, "/home/jlewi/tmp/graph.gexf");
  }
}
