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
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphNodeFilesIterator;
import contrail.graph.GraphTestUtil;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.sequences.KMerReadTag;
import contrail.stages.AvroCollectorMock;
import contrail.stages.QuickMergeAvro;
import contrail.util.FileHelper;

public class TestFilterByKMerTag {
  @Test
  public void testFilter() {
    ReporterMock reporter = new ReporterMock();

    FilterByKMerTag base = new FilterByKMerTag();
    FilterByKMerTag.Filter filter = new FilterByKMerTag.Filter();

    GraphNode node = GraphTestUtil.createNode("someNode", "ACTGT");
    KMerReadTag tag = new KMerReadTag("read", 10);
    node.setMertag(tag);

    JobConf job = new JobConf(filter.getClass());
    base.getParameterDefinitions().get("tags").addToJobConf(
        job, QuickMergeAvro.KMerTag(node.getData()));

    filter.configure(job);
    AvroCollectorMock<GraphNodeData> collectorMock =
        new AvroCollectorMock<GraphNodeData>();

    try {
      filter.map(node.clone().getData(), collectorMock, reporter);
      assertEquals(1, collectorMock.data.size());

      // Now repeat the test with a node that should be accepted.
      node.setMertag(new KMerReadTag("different_read", 100));
      collectorMock.data.clear();
      filter.map(node.clone().getData(), collectorMock, reporter);
      assertEquals(0, collectorMock.data.size());
    }
    catch (IOException exception){
      fail("IOException occured in map: " + exception.getMessage());
    }
  }

  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("TACTGGATT", 3);
    ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
    nodes.addAll(builder.getAllNodes().values());

    KMerReadTag tag = new KMerReadTag("compress", 0);
    nodes.get(0).setMertag(tag);

    File temp = FileHelper.createLocalTempDir();
    File avroFile = new File(temp, "graph.avro");

    GraphUtil.writeGraphToFile(avroFile, nodes);

    HashSet<String> targets = new HashSet<String>();
    targets.add(nodes.get(0).getNodeId());

    FilterNodes stage = new FilterNodes();
    File outputPath = new File(temp, "output");
    stage.setParameter("inputpath", avroFile.toString());
    stage.setParameter("outputpath", outputPath.toString());
    stage.setParameter("filter", FilterByKMerTag.class.getName());
    stage.setParameter(
        "filter_options",
        "--tags=" + QuickMergeAvro.KMerTag(nodes.get(0).getData()));
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
