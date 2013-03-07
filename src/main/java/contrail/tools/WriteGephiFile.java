/**
 * Licensed under the Apache License, Version 2.0 (the "License");
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

package contrail.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.R5Tag;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsUtil;
import contrail.stages.CompressibleNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * Covert the graph into an gephi formatted XML file which can then be loaded
 * in gephi.
 *
 * Doc about gexf format:
 * http://gexf.net/1.2draft/gexf-12draft-primer.pdf
 *
 * WARNING: Gephi appears to have problems reading files in "/tmp" so
 * write the file somewhere else.
 *
 * The input/output can be on any filesystem supported by the hadoop file api.
 *
 * TODO(jlewi): We should make color vary depending on the
 * node's length and coverage.
 */
public class WriteGephiFile extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(WriteGephiFile.class);

  // A mapping from node id's to integers used by gephi.
  private final HashMap<EdgeTerminal, Integer> node_id_map =
      new HashMap<EdgeTerminal, Integer>();

  // The next value to assign to a node;
  private int next_id = 0;

  private Document doc;

  // Hashmap mapping node attributes to their id values.
  private HashMap<String, String> nodeAttrIdMap;

  //Hashmap mapping edge attributes to their id values.
  private HashMap<String, String> edgeAttrIdMap;

  // The filesystem.
  private FileSystem fs;

  @Override
  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition start_node = new ParameterDefinition(
        "start_node", "(Optional) if supplied num_hops must also be given.",
        String.class, null);
    ParameterDefinition num_hops = new ParameterDefinition(
        "num_hops", "(Optional) Number of hops to take starting at start_node.",
        Integer.class, null);

    ParameterDefinition sequence = new ParameterDefinition(
        "sequence",
        "Include the contig sequence in the node metadata.", Boolean.class,
        false);

    ParameterDefinition r5 = new ParameterDefinition(
        "r5tags",
        "Include the r5tags.", Boolean.class,
        false);

    ParameterDefinition tmpCheck = new ParameterDefinition(
        "disallow_tmp",
        "(Only for unittest) disables the check to see if the outputpath is " +
        "/tmp.",
        Boolean.class, true);
    defs.put(start_node.getName(), start_node);
    defs.put(num_hops.getName(), num_hops);
    defs.put(tmpCheck.getName(), tmpCheck);
    defs.put(sequence.getName(), sequence);
    defs.put(r5.getName(), r5);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Create an XML element to represent the edge.
   * @param doc
   * @param edge_id
   * @param src
   * @param dest
   * @param readIds: List of strings containing the ids of the reads this edge
   *   came from.
   * @return
   */
  private Element createElementForEdge(
      Document doc, int edge_id, EdgeTerminal src, EdgeTerminal dest,
      ArrayList<String> readIds) {
    Element xml_edge = doc.createElement("edge");
    Integer this_node_id = IdForTerminal(src);

    xml_edge.setAttribute("id", Integer.toString(edge_id));
    edge_id++;

    xml_edge.setAttribute("source", this_node_id.toString());
    Integer target_id = IdForTerminal(dest);
    xml_edge.setAttribute("target", target_id.toString());

    xml_edge.setAttribute("type", "directed");
    xml_edge.setAttribute(
        "label",
        StrandsUtil.form(src.strand,  dest.strand).toString());

    // Set all the attributes.
    Element attributeRoot = doc.createElement("attvalues");
    xml_edge.appendChild(attributeRoot);
    for (Entry<String, String> entry : edgeAttrIdMap.entrySet()) {
      Element attribute = doc.createElement("attvalue");
      attribute.setAttribute("for", entry.getValue());
      String value;
      if (entry.getKey().equals("read-ids")) {
        Collections.sort(readIds);
        value = StringUtils.join(readIds, ",");
      } else {
        throw new RuntimeException(
            "No handler for attribute:" + entry.getKey());
      }
      attribute.setAttribute("value", value);
      attributeRoot.appendChild(attribute);
    }
    return xml_edge;
  }

  private void AddTerminalToIndex(EdgeTerminal terminal) {
    node_id_map.put(terminal, next_id);
    ++next_id;
  }

  private void AddNodesToIndex(Collection<GraphNode> nodes) {
    // A node for both the forward and reverse strands.
    EdgeTerminal terminal;
    for (GraphNode node: nodes) {
      terminal = new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD);
      node_id_map.put(terminal, next_id);
      ++next_id;
      node_id_map.put(terminal.flip(), next_id);
      ++next_id;
    }
  }

  private Integer IdForTerminal(EdgeTerminal terminal) {
    return node_id_map.get(terminal);
  }

  private Element CreateTerminal(EdgeTerminal terminal, GraphNode node) {
    Element xml_node = doc.createElement("node");
    Integer this_node_id = IdForTerminal(terminal);

    xml_node.setAttribute("id", this_node_id.toString());

    xml_node.setAttribute(
        "label", terminal.nodeId + ":" +
        terminal.strand.toString().substring(0, 1));

    // Set all the attributes.
    Element attributeRoot = doc.createElement("attvalues");
    xml_node.appendChild(attributeRoot);
    for (Entry<String, String> entry : nodeAttrIdMap.entrySet()) {
      Element attribute = doc.createElement("attvalue");
      attribute.setAttribute("for", entry.getValue());
      String value;
      if (entry.getKey().equals("out-degree")) {
        value = Integer.toString(
            node.degree(terminal.strand, EdgeDirection.OUTGOING));
      } else if (entry.getKey().equals("in-degree")) {
        value = Integer.toString(
            node.degree(terminal.strand, EdgeDirection.INCOMING));
      } else if (entry.getKey().equals("node-id")) {
        value = node.getNodeId();
      } else if (entry.getKey().equals("coverage")) {
        value = Float.toString(node.getCoverage());
      } else if (entry.getKey().equals("length")) {
        value = Integer.toString(node.getSequence().size());
      } else if (entry.getKey().equals("sequence")) {
        value = node.getSequence().toString();
      } else if (entry.getKey().equals("r5tags")) {
        // Should we include the offset the tag?
        ArrayList<String> tags = new ArrayList<String>();
        for (R5Tag tag : node.getData().getR5Tags()) {
          tags.add(tag.getTag().toString());
        }
        Collections.sort(tags);
        value = StringUtils.join(tags, ",");
      } else {
        throw new RuntimeException(
            "No handler for attribute:" + entry.getKey());
      }
      attribute.setAttribute("value", value);
      attributeRoot.appendChild(attribute);
    }

    Element xmlSize = doc.createElement("size");
    double size = Math.log10(node.getSequence().size() + 1);
    xmlSize.setAttribute("value", Double.toString(size));
    xml_node.appendChild(xmlSize);
    return xml_node;
  }

  public void writeGraph(Map<String, GraphNode> nodes, String xml_file) {
    if ((Boolean)stage_options.get("disallow_tmp")) {
      if (xml_file.startsWith("/tmp")) {
        throw new RuntimeException(
            "Don't write the file to '/tmp' gephi has problems with that.");
      }
    }
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = null;
    try {
       dBuilder = dbFactory.newDocumentBuilder();
    } catch (Exception exception) {
        sLogger.error("Exception:" + exception.toString());
    }
    doc = dBuilder.newDocument();

    Element gexf_root = doc.createElement("gexf");
    doc.appendChild(gexf_root);
    Element root = doc.createElement("graph");
    gexf_root.appendChild(root);

    nodeAttrIdMap = new HashMap<String, String>();
    {
      // Declare some attributes for nodes
      Element attributes = doc.createElement("attributes");
      root.appendChild(attributes);
      attributes.setAttribute("class", "node");

      List<String> fields = Arrays.asList(new String[]{
          "node-id", "out-degree", "in-degree", "length", "coverage"});


      if ((Boolean)stage_options.get("sequence")) {
        fields.add("sequence");
      }

      if ((Boolean)stage_options.get("r5tags")) {
        fields.add("r5tags");
      }


      for (int i = 0; i < fields.size(); ++i) {
        nodeAttrIdMap.put(fields.get(i), Integer.toString(i));
      }

      for (Entry<String, String> entry : nodeAttrIdMap.entrySet()) {
        Element attribute = doc.createElement("attribute");
        attribute.setAttribute("id", entry.getValue());
        attribute.setAttribute("title", entry.getKey());
        attribute.setAttribute("type", "string");
        attributes.appendChild(attribute);
      }
    }

    edgeAttrIdMap = new HashMap<String, String> ();
    {
      // Declare some attributes for edge
      Element attributes = doc.createElement("attributes");
      root.appendChild(attributes);
      attributes.setAttribute("class", "edge");

      edgeAttrIdMap.put("read-ids", "0");

      for (Entry<String, String> entry : edgeAttrIdMap.entrySet()) {
        Element attribute = doc.createElement("attribute");
        attribute.setAttribute("id", entry.getValue());
        attribute.setAttribute("title", entry.getKey());
        attribute.setAttribute("type", "string");
        attributes.appendChild(attribute);

        // Set the default value to the empty string because if no value
        // is supplied and the attribute isn't set its an error.
        Element defaultNode = doc.createElement("default");
        defaultNode.setTextContent("");
        attribute.appendChild(defaultNode);
      }
    }

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
    AddNodesToIndex(nodes.values());

    for (GraphNode node: nodes.values()) {
      for (DNAStrand strand : DNAStrand.values()) {
        EdgeTerminal terminal = new EdgeTerminal(node.getNodeId(), strand);

        Element xml_node = CreateTerminal(terminal, node);
        xml_nodes.appendChild(xml_node);

        List<EdgeTerminal> edges =
            node.getEdgeTerminals(strand, EdgeDirection.OUTGOING);
        for (EdgeTerminal other_terminal: edges){
          // If the node for the edge isn't provided skip it.
          // TODO(jlewi): It would be nice to visually indicate those
          // terminals which are actually terminal's in the node (i.e
          // we don't have GraphNode's for them.)
          if (IdForTerminal(other_terminal) == null) {
            AddTerminalToIndex(other_terminal);
            Element new_node = CreateTerminal(other_terminal, node);
            xml_nodes.appendChild(new_node);
          }
          ArrayList<String> readIds = new ArrayList<String>();
          for (CharSequence tag : node.getTagsForEdge(strand, other_terminal)) {
            readIds.add(tag.toString());
          }
          Element xml_edge = createElementForEdge(
              doc, ++edge_id, terminal, other_terminal, readIds);
          xml_edges.appendChild(xml_edge);
        }

        // TODO(jlewi): Should we also plot any incoming edges if the
        // edge terminates on a node which isn't in nodes?
      }
    }

    // Write the content into xml file
    try {
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      DOMSource source = new DOMSource(doc);
      FSDataOutputStream outStream = fs.create(new Path(xml_file), true);
      StreamResult result = new StreamResult(outStream);
      transformer.transform(source, result);
      outStream.close();
    } catch (Exception exception) {
      sLogger.error("Exception:" + exception.toString());
    }
  }

  private enum InputRecordTypes {
    GraphNodeData,
    CompressibleNodeData,
    Unknown
  };

  private InputRecordTypes determineInputType (Path path) {
    // Determine the input type by reading the first record in one of the
    // file.
    GenericDatumReader reader = new GenericDatumReader<GenericRecord>();
    try {
      FSDataInputStream inStream = fs.open(path);

      DataFileStream<GenericRecord> avro_stream =
          new DataFileStream<GenericRecord>(inStream, reader);

      if (!avro_stream.hasNext()) {
        throw new RuntimeException(
            "Couldn't determine the input record type because no records could be read.");
      }
      GenericRecord record = avro_stream.next();
      inStream.close();
      if (record.getSchema().getName().equals("GraphNodeData")) {
        return InputRecordTypes.GraphNodeData;
      } else if (record.getSchema().getName().equals("CompressibleNodeData")) {
        return InputRecordTypes.CompressibleNodeData;
      } else {
        throw new RuntimeException(String.format(
            "%s is not a valid schema for the input",
            record.getSchema().getName()));
      }
    } catch (IOException exception) {
      throw new RuntimeException(
          "There was a problem reading the nodes the graph to an avro file." +
          " Exception:" + exception.getMessage());
    }
  }

  private HashMap<String, GraphNode> readGraphNodes(List<Path> inputFiles) {
    // Read the nodes from a file where the record type is GraphNodeData.
    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    try {
      for (Path inFile : inputFiles) {
        FSDataInputStream inStream = fs.open(inFile);
        SpecificDatumReader<GraphNodeData> reader =
            new SpecificDatumReader<GraphNodeData>();
        DataFileStream<GraphNodeData> avro_stream =
            new DataFileStream<GraphNodeData>(inStream, reader);
        while(avro_stream.hasNext()) {
          GraphNodeData data  = avro_stream.next();
          GraphNode node = new GraphNode(data);

          nodes.put(node.getNodeId(), node);
        }
        inStream.close();
      }
    } catch (IOException exception) {
      throw new RuntimeException(
          "There was a problem reading the nodes the graph to an avro file." +
              " Exception:" + exception.getMessage());
    }
    return nodes;
  }

  private HashMap<String, GraphNode> readCompressibleNodes(
      List<Path> inputFiles) {
    // Read the nodes from a file where the record type is GraphNodeData.
    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    try {
      for (Path inFile : inputFiles) {
        FSDataInputStream inStream = fs.open(inFile);

        SpecificDatumReader<CompressibleNodeData> reader =
            new SpecificDatumReader<CompressibleNodeData>();
        DataFileStream<CompressibleNodeData> avro_stream =
            new DataFileStream<CompressibleNodeData>(inStream, reader);
        while(avro_stream.hasNext()) {
          CompressibleNodeData data  = avro_stream.next();
          GraphNode node = new GraphNode(data.getNode());

          nodes.put(node.getNodeId(), node);
        }
        inStream.close();
      }
    } catch (IOException exception) {
      throw new RuntimeException(
          "There was a problem reading the nodes the graph to an avro file." +
              " Exception:" + exception.getMessage());
    }
    return nodes;
  }


  private HashMap<String, GraphNode> readNodes() {
    String inputPathStr = (String) stage_options.get("inputpath");
    sLogger.info(" - input: "  + inputPathStr);;

    // Check if path is a directory.
    Path inputPath = new Path(inputPathStr);

    FileStatus[] fileStates = null;
    try {
      fileStates = fs.globStatus(new Path(inputPathStr));
    } catch (IOException e) {
      sLogger.fatal("Could not get file status for inputpath:" + inputPath, e);
      System.exit(-1);
    }

    ArrayList<Path> inputFiles = new ArrayList<Path>();

    for (FileStatus status : fileStates) {
     if (status.isDir()) {
	 sLogger.info("Skipping directory:" + status.getPath());
         continue;
      }
      sLogger.info("Input file:" + status.getPath()) ;
      inputFiles.add(status.getPath());
    }

    if (inputFiles.size() == 0) {
      throw new RuntimeException("Didn't find any graph files.");
    }

    InputRecordTypes inputType = determineInputType(inputFiles.get(0));

    if (inputType == InputRecordTypes.GraphNodeData) {
      return readGraphNodes(inputFiles);
    }

    if (inputType == InputRecordTypes.CompressibleNodeData) {
      return readCompressibleNodes(inputFiles);
    }
    throw new RuntimeException("No handler for this schema type");
  }

  /**
   * Find the subgraph by starting at the indicated node and walking the
   * specified number of hops.
   */
  private HashMap<String, GraphNode> getSubGraph(
      HashMap<String, GraphNode> nodes) {
    HashMap<String, GraphNode> subGraph = new HashMap<String, GraphNode>();

    // Use two lists so we can keep track of the hops.
    HashSet<String> thisHop = new HashSet<String>();
    HashSet<String> nextHop = new HashSet<String>();

    String start_node = (String) stage_options.get("start_node");

    if (!nodes.containsKey(start_node)) {
      throw new RuntimeException(
          "The input doesn't contain the node: " + start_node);
    }

    int num_hops = (Integer) stage_options.get("num_hops");
    int hop = 0;
    thisHop.add(start_node);
    while (hop <= num_hops && thisHop.size() > 0) {
      // Fetch each node in thisHop.
      for (String nodeId : thisHop) {
        if (subGraph.containsKey(nodeId)) {
          continue;
        }
        if (!nodes.containsKey(nodeId)) {
          // The node isn't in the graph. This can happen if we are only
          // displaying part of the graph.
          // TODO(jlewi): The visualization should indicate nodes which
          // don't have all their edges shown.
          continue;
        }
        subGraph.put(nodeId, nodes.get(nodeId));
        GraphNode target = nodes.get(nodeId);

        nextHop.addAll(nodes.get(nodeId).getNeighborIds());
      }
      thisHop.clear();
      thisHop.addAll(nextHop);
      nextHop.clear();
      ++hop;
    }
    return subGraph;
  }

  @Override
  public RunningJob runJob() throws Exception {
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal(e.getMessage(), e);
      System.exit(-1);
    }
    // Check for missing arguments.
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String outputPath = (String) stage_options.get("outputpath");
    sLogger.info(" - output: " + outputPath);

    HashMap<String, GraphNode> nodes = readNodes();

    //TODO(jlewi): Filter the nodes to get the subgraph of interest.
    if (stage_options.containsKey("num_hops") !=
        stage_options.containsKey("start_node")) {
      throw new RuntimeException(
          "You must supply num_hops and start_node if you want to draw only " +
          "part of the graph.");
    }

    if (stage_options.containsKey("start_node")) {
      nodes = getSubGraph(nodes);
    }
    writeGraph(nodes, outputPath);
    sLogger.info("Wrote: " + outputPath);

    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WriteGephiFile(), args);
    System.exit(res);
  }
}
