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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.R5Tag;
import contrail.sequences.DNAStrand;
import contrail.sequences.StrandsUtil;
import contrail.stages.CompressibleNodeData;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.NotImplementedException;
import contrail.stages.ParameterDefinition;

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
 *
 * TODO(jeremy@lewi.us): Make this a subclass of WriteGephiFileBase.
 */
public class WriteGephiFile extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(WriteGephiFile.class);

  // A mapping from node id's to integers used by gephi.
  private final HashMap<EdgeTerminal, Integer> node_id_map =
      new HashMap<EdgeTerminal, Integer>();

  // The next value to assign to a node;
  private int next_id = 0;

  private XMLStreamWriter writer;

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
        String.class, "");
    ParameterDefinition num_hops = new ParameterDefinition(
        "num_hops", "(Optional) Number of hops to take starting at start_node.",
        Integer.class, 0);

    ParameterDefinition sequence = new ParameterDefinition(
        "sequence",
        "Include the contig sequence in the node metadata.", Boolean.class,
        false);

    ParameterDefinition r5 = new ParameterDefinition(
        "r5tags",
        "Include the r5tags.", Boolean.class,
        false);

    ParameterDefinition readIds = new ParameterDefinition(
        "read_ids",
        "Include the read ids associated with each edge.", Boolean.class,
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
    defs.put(readIds.getName(), readIds);
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
  private void writeEdge(
      int edge_id, EdgeTerminal src, EdgeTerminal dest,
      ArrayList<String> readIds) {
    try {
      writer.writeStartElement("edge");
      Integer this_node_id = IdForTerminal(src);

      writer.writeAttribute("id", Integer.toString(edge_id));
      edge_id++;

      writer.writeAttribute("source", this_node_id.toString());
      Integer target_id = IdForTerminal(dest);
      writer.writeAttribute("target", target_id.toString());

      writer.writeAttribute("type", "directed");
      writer.writeAttribute(
          "label",
          StrandsUtil.form(src.strand,  dest.strand).toString());

      // Set all the attributes.
      writer.writeStartElement("attvalues");
      for (Entry<String, String> entry : edgeAttrIdMap.entrySet()) {
        writer.writeStartElement("attvalue");
        writer.writeAttribute("for", entry.getValue());
        String value;
        if (entry.getKey().equals("read-ids")) {
          Collections.sort(readIds);
          value = StringUtils.join(readIds, ",");
        } else {
          throw new RuntimeException(
              "No handler for attribute:" + entry.getKey());
        }
        writer.writeAttribute("value", value);
        writer.writeEndElement();
      }
      writer.writeEndElement(); // attrvalues
      writer.writeEndElement(); // edge
    } catch (XMLStreamException e) {
      sLogger.fatal("XML write error", e);
      System.exit(-1);
    }
  }

  private void AddTerminalToIndex(EdgeTerminal terminal) {
    node_id_map.put(terminal, next_id);
    ++next_id;
  }

  private void AddNodeToIndex(GraphNode node) {
    // Add a node for both the forward and reverse strands.
    EdgeTerminal terminal;
    terminal = new EdgeTerminal(node.getNodeId(), DNAStrand.FORWARD);
    node_id_map.put(terminal, next_id);
    ++next_id;
    node_id_map.put(terminal.flip(), next_id);
    ++next_id;
  }

  private Integer IdForTerminal(EdgeTerminal terminal) {
    return node_id_map.get(terminal);
  }

  private void writeTerminal(EdgeTerminal terminal, GraphNode node) {
    try{
      writer.writeStartElement("node");
      Integer this_node_id = IdForTerminal(terminal);

      writer.writeAttribute("id", this_node_id.toString());

      writer.writeAttribute(
          "label", terminal.nodeId + ":" +
              terminal.strand.toString().substring(0, 1));

      // Set all the attributes.
      writer.writeStartElement("attvalues");

      for (Entry<String, String> entry : nodeAttrIdMap.entrySet()) {
        writer.writeStartElement("attvalue");
        writer.writeAttribute("for", entry.getValue());
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
        writer.writeAttribute("value", value);
        writer.writeEndElement(); // attvalue
      }
      writer.writeEndElement(); //attvalues

      writer.writeStartElement("size");
      double size = Math.log10(node.getSequence().size() + 1);
      writer.writeAttribute("value", Double.toString(size));
      writer.writeEndElement(); // size
      writer.writeEndElement(); // node
    } catch (XMLStreamException e) {
      sLogger.fatal("XML write error", e);
      System.exit(-1);
    }
  }

  public void writeGraph(Iterable <GraphNode> nodes, String xml_file) {
    if ((Boolean)stage_options.get("disallow_tmp")) {
      if (xml_file.startsWith("/tmp")) {
        throw new RuntimeException(
            "Don't write the file to '/tmp' gephi has problems with that.");
      }
    }

    // Write the content into xml file
    FSDataOutputStream outStream = null;
    XMLOutputFactory xof = null;
    try {
      outStream = fs.create(new Path(xml_file), true);
      xof = XMLOutputFactory.newInstance();
      writer = xof.createXMLStreamWriter(outStream);
    } catch (Exception exception) {
      sLogger.error("Exception:" + exception.toString());
    }

    try {
    writer.writeStartDocument();
    writer.writeStartElement("gexf");
    writer.writeStartElement("graph");
    writer.writeAttribute("mode", "static");
    writer.writeAttribute("defaultedgetype", "directed");

    nodeAttrIdMap = new HashMap<String, String>();
    {
      // Declare some attributes for nodes
      writer.writeStartElement("attributes");
      writer.writeAttribute("class", "node");

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
        writer.writeStartElement("attribute");
        writer.writeAttribute("id", entry.getValue());
        writer.writeAttribute("title", entry.getKey());
        writer.writeAttribute("type", "string");
        writer.writeEndElement();
      }
      writer.writeEndElement(); // attributes;
    }

    edgeAttrIdMap = new HashMap<String, String> ();
    {
      // Declare some attributes for edge
      writer.writeStartElement("attributes");
      writer.writeAttribute("class", "edge");

      List<String> fields = new ArrayList<String>();

      if ((Boolean)stage_options.get("read_ids")) {
        fields.add("read-ids");
      }

      for (int i = 0; i < fields.size(); ++i) {
        edgeAttrIdMap.put(fields.get(i), Integer.toString(i));
      }

      for (Entry<String, String> entry : edgeAttrIdMap.entrySet()) {
        writer.writeStartElement("attribute");
        writer.writeAttribute("id", entry.getValue());
        writer.writeAttribute("title", entry.getKey());
        writer.writeAttribute("type", "string");

        // Set the default value to the empty string because if no value
        // is supplied and the attribute isn't set its an error.
        writer.writeStartElement("default");
        writer.writeCharacters("");
        writer.writeEndElement(); // default
        writer.writeEndElement(); // attribute
      }
      writer.writeEndElement(); // attributes;
    }

    writer.writeStartElement("nodes");
    int count = 0;
    int progressInterval = 1000;
    for (GraphNode node: nodes) {
      ++count;
      if (count % progressInterval == 0) {
        sLogger.info(String.format("Processing node: %d", count));
      }
      AddNodeToIndex(node);
      for (DNAStrand strand : DNAStrand.values()) {
        EdgeTerminal terminal = new EdgeTerminal(node.getNodeId(), strand);
        writeTerminal(terminal, node);
      }
    }
    writer.writeEndElement(); // nodes

    writer.writeStartElement("edges");
    count = 0;
    // We assign each edge a unique id.
    int edge_id = 0;
    for (GraphNode node : nodes) {
      ++count;
      if (count % progressInterval == 0) {
        sLogger.info(String.format("Processing edges for node: %d", count));
      }
      for (DNAStrand strand : DNAStrand.values()) {
        EdgeTerminal terminal = new EdgeTerminal(node.getNodeId(), strand);
        List<EdgeTerminal> edges =
            node.getEdgeTerminals(strand, EdgeDirection.OUTGOING);
        for (EdgeTerminal other_terminal: edges){
          // If the node for the edge isn't provided skip it.
          // TODO(jlewi): It would be nice to visually indicate those
          // terminals which are actually terminal's in the node (i.e
          // we don't have GraphNode's for them.)
          if (IdForTerminal(other_terminal) == null) {
            continue;
            //AddTerminalToIndex(other_terminal);
            //Element new_node = CreateTerminal(other_terminal, node);
            //xml_nodes.appendChild(new_node);
          }
          ArrayList<String> readIds = new ArrayList<String>();
          for (CharSequence tag : node.getTagsForEdge(strand, other_terminal)) {
            readIds.add(tag.toString());
          }
          writeEdge(++edge_id, terminal, other_terminal, readIds);
        }

        // TODO(jlewi): Should we also plot any incoming edges if the
        // edge terminates on a node which isn't in nodes?
      }
    }
    writer.writeEndElement(); // edges

    writer.writeEndElement(); // graph
    writer.writeEndElement(); // gexf
    writer.flush();
    writer.close();
    } catch (XMLStreamException e) {
      sLogger.fatal("XML write error", e);
      System.exit(-1);
    }

    try {
      outStream.close();
    } catch(IOException e) {
      sLogger.fatal("Error closing the stream.", e);
      System.exit(-1);
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

  private class InputInfo {
    public ArrayList<Path> files;
    public InputRecordTypes type;

    public InputInfo(ArrayList<Path> inputFiles,  InputRecordTypes inputType) {
      files = inputFiles;
      type = inputType;
    }
  }

  private InputInfo getInputInfo() {
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

    return new InputInfo(inputFiles, inputType);
  }

  private HashMap<String, GraphNode> readNodes() {
    InputInfo info = getInputInfo();

    if (info.type == InputRecordTypes.GraphNodeData) {
      return readGraphNodes(info.files);
    }

    if (info.type == InputRecordTypes.CompressibleNodeData) {
      return readCompressibleNodes(info.files);
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
  protected void stageMain() {
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

    Iterable<GraphNode> nodesToPlot;
    String start_nodes = (String) stage_options.get("start_node");
    if (!start_nodes.isEmpty()) {
      // If we're plotting a subgraph then we need to be able to load
      // the particular node. We should really use an indexed AvroFile
      // to facilitate quick lookups.
      HashMap<String, GraphNode> nodes = readNodes();

      //TODO(jlewi): Filter the nodes to get the subgraph of interest.
      if (stage_options.containsKey("num_hops") !=
          stage_options.containsKey("start_node")) {
        throw new RuntimeException(
            "You must supply num_hops and start_node if you want to draw only " +
            "part of the graph.");
      }

      nodes = getSubGraph(nodes);
      nodesToPlot = nodes.values();
    } else {
      // We're plotting the entire graph so use an iterator so we don't
      // need to load the entire graph into memory.
      InputInfo info = getInputInfo();
      if (info.type != InputRecordTypes.GraphNodeData) {
        sLogger.fatal(
            "Currently only GraphNodeData avro files are supported",
            new NotImplementedException());
      }

      nodesToPlot = new GraphNodeFilesIterator(getConf(), info.files);
    }

    writeGraph(nodesToPlot, outputPath);

    sLogger.info("Wrote: " + outputPath);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new WriteGephiFile(), args);
    System.exit(res);
  }
}
