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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import contrail.graph.GraphError;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphUtil;
import contrail.stages.QuickMergeAvro;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/**
 * This test uses a graph which caused problems with the Staph dataset
 * on 2013 10 23.
 *
 * This is a data driven test. The data is a json file containing the
 * GraphNode's representing the graph. The json file should be colocated in
 * the same package as the test.
 *
 * The bug was caused by the graph:
 * Z->A
 * A->B->C->D->A
 * In this case QuickMerge will try to merge chain B->-C->-D.
 * The bug was in updating the edges in A to the merged nodes.
 */
public class TestStaph20131023 {
  private static final Logger sLogger =
      Logger.getLogger(TestStaph20131023.class);

  private final int K = 41;

  // The name of the file resource to use. This resource should be the json
  // file which contains the subgraph extracted from the staph dataset.
  // The graph should have been pruned so that it is a valid graph.
  private static String graphResourcePath =
      "contrail/integration/staph_2013_1023_subgraph.json";

  /**
   * Read and validate the json records.
   */
  private HashMap<String, GraphNode> readJsonInput() {
    InputStream inStream = this.getClass().getClassLoader().getResourceAsStream(
        graphResourcePath);

    if (inStream == null) {
      fail("Could not find resource:" + graphResourcePath);
    }

    Schema schema = new GraphNodeData().getSchema();
    Configuration conf = new Configuration();
    ArrayList<GraphNodeData> records =
        AvroFileUtil.readJsonRecords(inStream, schema);

    HashMap<String, GraphNode> graph = new HashMap<String, GraphNode>();
    for (GraphNodeData data : records) {
      graph.put(data.getNodeId().toString(), new GraphNode(data));
    }

    // Make sure the input graph is valid.
    assertTrue(isValid(graph));
    return graph;
  }

  private boolean isValid(HashMap<String, GraphNode> nodes) {
    List<GraphError> errors = GraphUtil.validateGraph(nodes, K);
    for (GraphError error : errors) {
      System.out.println(error.toString());
    }
    return errors.isEmpty();
  }

  @Test
  public void testGraph() {
    HashMap<String, GraphNode> graph = readJsonInput();

    File tempDir = FileHelper.createLocalTempDir();
    String inputPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "input");
    String mergedPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "merged");

    Configuration conf = new Configuration();
    GraphUtil.writeGraphToPath(
        conf,
        new Path(FilenameUtils.concat(inputPath, "graph.avro")),
        graph.values());

    // Lets run quick merge.
    QuickMergeAvro quickMerge = new QuickMergeAvro();
    quickMerge.setParameter("inputpath", inputPath);
    quickMerge.setParameter("outputpath", mergedPath);
    quickMerge.setParameter("K", K);
    if (!quickMerge.execute()) {
      fail("QuickMerge failed.");
    }

    GraphNodeFilesIterator outputs = GraphNodeFilesIterator.fromGlob(
        new Configuration(),
        FilenameUtils.concat(mergedPath, "*.avro"));

    HashMap<String, GraphNode> mergedGraph = new HashMap<String, GraphNode>();
    for (GraphNode n : outputs) {
      mergedGraph.put(n.getNodeId(), n.clone());
    }

    // Verify the graph is valid.
    assertTrue(isValid(mergedGraph));

    long numCompressedChains = quickMerge.getCounter(
        QuickMergeAvro.NUM_COMPRESSED_CHAINS.group,
        QuickMergeAvro.NUM_COMPRESSED_CHAINS.tag);

    assertEquals(4, numCompressedChains);

    long numNodesCompressed = quickMerge.getCounter(
        QuickMergeAvro.NUM_COMPRESSED_NODES.group,
        QuickMergeAvro.NUM_COMPRESSED_NODES.tag);

    assertEquals(14, numNodesCompressed);

    long numOutputNodes = quickMerge.getNumReduceOutputRecords();
    assertEquals(7, numOutputNodes);
  }
}
