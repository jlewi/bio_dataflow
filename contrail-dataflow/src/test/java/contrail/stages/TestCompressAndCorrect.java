/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.ContrailLogger;
import contrail.util.FileHelper;

public class TestCompressAndCorrect extends CompressAndCorrect {
  @Before
  public void setup() {
    // Setup the logger so we don't simply exit on failure.
    ContrailLogger.setExitOnFatal(false);
    ContrailLogger.setTestMode(true);
  }

  private void writeGraph(File avroFile, Map<String, GraphNode> nodes) {
    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datumWriter =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (GraphNode node: nodes.values()) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }
  }

  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGATT", 3);

    // Add some tips.
    builder.addEdge("ATT", "TTG", 2);
    builder.addEdge("ATT", "TTC", 2);
    int tipLength = 100;

    // Add a bubble.
    builder.addEdge("CCAA", "AATTG", 2);
    builder.addEdge("CCAA", "AAGTG", 2);
    builder.addEdge("AATTG", "TGGG", 2);
    builder.addEdge("AAGTG", "TGGG", 2);
    builder.findNodeForSequence("AATTG").setCoverage(10);
    builder.findNodeForSequence("AAGTG").setCoverage(1);
    float bubbleEditRate = 1.0f / 10.0f;
    int bubbleLengthThreshold = 100;

    File temp = FileHelper.createLocalTempDir();
    File avroFile = new File(temp, "graph.avro");

    writeGraph(avroFile, builder.getAllNodes());

    // Run it.
    CompressAndCorrect stage = new CompressAndCorrect();
    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());

    File output_path = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
       "--outputpath=" + output_path.toURI().toString(),
       "--cleanup=true",
       "--K=3", "--localnodes=3", "--tiplength=" + tipLength,
       "--bubble_edit_rate=" + bubbleEditRate,
       "--bubble_length_threshold=" + bubbleLengthThreshold,
       "--compute_stats=false", "--length_thresh=5",
       "--low_cov_thresh=5"};

    // Catch the following after debugging.
    try {
      stage.run(args);
    } catch (Exception exception) {
      exception.printStackTrace();
      fail("Exception occured:" + exception.getMessage());
    }
  }
}