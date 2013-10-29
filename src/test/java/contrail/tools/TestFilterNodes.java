/*
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
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.FileHelper;

public class TestFilterNodes {
  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("TACTGGATT", 3);

    File temp = FileHelper.createLocalTempDir();
    File avroFile = new File(temp, "graph.avro");

    GraphUtil.writeGraphToFile(avroFile, builder.getAllNodes().values());

    HashSet<String> targets = new HashSet<String>();
    Iterator<GraphNode> iter = builder.getAllNodes().values().iterator();
    targets.add(iter.next().getNodeId());
    targets.add(iter.next().getNodeId());

    FilterNodes stage = new FilterNodes();
    File outputPath = new File(temp, "output");
    stage.setParameter("inputpath", avroFile.toString());
    stage.setParameter("outputpath", outputPath.toString());
    stage.setParameter("filter", FilterByName.class.getName());
    stage.setParameter(
        "filter_options",
        "--nodes=" + StringUtils.join(targets, ","));
    assertTrue(stage.execute());

    GraphNodeFilesIterator outputs = GraphNodeFilesIterator.fromGlob(
        new Configuration(),
        FilenameUtils.concat(outputPath.toString(), "*.avro"));

    HashSet<String> outIds = new HashSet<String>();
    for (GraphNode output : outputs) {
      outIds.add(output.getNodeId());
    }
    assertEquals(targets, outIds);
  }
}
