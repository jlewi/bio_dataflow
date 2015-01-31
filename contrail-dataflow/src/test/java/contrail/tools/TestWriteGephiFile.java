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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.graph.R5Tag;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.DNAStrand;
import contrail.util.FileHelper;

/**
 * Unittest for creating a gephi file from a graph.
 *
 * The unittest only checks the code executes without errors. To check
 * the produced XML file is valid you have to manually load it into gephi.
 */
public class TestWriteGephiFile {

  // Important: The unittest creates the gephi file in /tmp. Gephi has a bug
  // which causes any file in /tmp to be truncated to length zero when its
  // opened. So you need to move the file out of tmp before trying ot open it.
  @Test
  public void testGephiFile () {
    // Create a graph
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGTCG", 4);

    // Fill in the attributes for at least one node so we no its working.
    GraphNode node = builder.findNodeForSequence("ACTG");
    R5Tag r5tag = new R5Tag();
    r5tag.setOffset(10);
    r5tag.setStrand(DNAStrand.FORWARD);
    r5tag.setTag("r5tag");
    node.getData().getR5Tags().add(r5tag);

    node.getData().getNeighbors().get(0).getEdges().get(0).getReadTags().add(
        "edge-read-id");

    File tempDir = FileHelper.createLocalTempDir();
    File avroFile = new File(tempDir, "graph.avro");
    GraphUtil.writeGraphToFile(avroFile, builder.getAllNodes().values());

    WriteGephiFile stage = new WriteGephiFile();
    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("inputpath", avroFile.getPath());
    String outPath = FilenameUtils.concat(tempDir.getPath(), "graph.gexf");
    parameters.put("outputpath", outPath);

    // Disable the check to see if the output is in tmp because for the
    // unittest it most likely will be.
    parameters.put("disallow_tmp", false);
    stage.setParameters(parameters);
    stage.setConf(new JobConf());
    stage.setParameter("start_node", node.getNodeId());
    stage.setParameter("num_hops", 2);
    assertTrue(stage.execute());
  }
}
