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
package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphTestUtil;
import contrail.io.AvroFileContentsIterator;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestGroupByComponentId {
  @Test
  public void testMR() {
    // Create some graph nodes.
    GraphNode nodeA = GraphTestUtil.createNode("nodeA", "ACTGCT");
    GraphNode nodeB = GraphTestUtil.createNode("nodeB", "ACTGCT");

    File temp = FileHelper.createLocalTempDir();
    String graphPath = FilenameUtils.concat(
        temp.getAbsolutePath(), "nodes.avro");

    // Write componen pairs.
    Pair<CharSequence, GraphNodeData> pairA =
        new Pair<CharSequence, GraphNodeData>("1", nodeA.getData());
    Pair<CharSequence, GraphNodeData> pairB =
        new Pair<CharSequence, GraphNodeData>("1", nodeB.getData());

    AvroFileUtil.writeRecords(
        new Configuration(), new Path(graphPath), Arrays.asList(pairA, pairB));

    GroupByComponentId stage = new GroupByComponentId();
    stage.setParameter(
        "inputpath", graphPath);
    stage.setParameter(
        "outputpath", FilenameUtils.concat(temp.getPath(), "outputpath"));

    assertTrue(stage.execute());

    // Open the output.
    AvroFileContentsIterator<List<GraphNodeData>> outputs =
        AvroFileContentsIterator.fromGlob(
            new Configuration(),
            FilenameUtils.concat(temp.getPath(), "outputpath/part*avro"));

    List<GraphNodeData> component = outputs.next();
    assertEquals(2, component.size());
  }
}
