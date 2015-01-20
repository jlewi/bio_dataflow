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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.AvroFileUtil;

/**
 * Create some fake graphs for testing ValidateGraph and ValidateGraphDataflow.
 *
 */
public class ValidateGraphTestGraphs {
  private static final Logger sLogger = LoggerFactory.getLogger(
      ValidateGraphTestGraphs.class);
  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path of the file to write to")
    String getOutput();
    void setOutput(String value);

    @Description("Which graph to write.")
    String getGraph();
    void setGraph(String value);
  }

  public Collection<GraphNodeData> buildValidGraph() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTG", 3);

//    // Add some tips.
//    builder.addEdge("ATT", "TTG", 2);
//    builder.addEdge("ATT", "TTC", 2);

    ArrayList<GraphNodeData> data = new ArrayList<GraphNodeData>();

    for (GraphNode node : builder.getAllNodes().values()) {
      data.add(node.getData());
    }
    return data;
  }

  public Collection<GraphNodeData> buildInvalidGraph() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGATT", 3);

    // Add some tips.
    builder.addEdge("ATT", "TTG", 2);
    builder.addEdge("ATT", "TTC", 2);

    ArrayList<GraphNodeData> data = new ArrayList<GraphNodeData>();
    for (GraphNode node : builder.getAllNodes().values()) {
      data.add(node.getData());
    }
    // Remove one of the nodes to make it invalid.
    data.remove(data.size() - 1);
    return data;
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).as(
        Options.class);
    String outputPath = options.getOutput();
    String graph = options.getGraph();
    if (outputPath == null) {
      throw new IllegalArgumentException("Must specify --output");
    }

    if (graph == null) {
      throw new IllegalArgumentException("Must specify --graph");
    }
    ValidateGraphTestGraphs testGraph = new ValidateGraphTestGraphs();

    Collection<GraphNodeData> nodes = null;
    if (graph.equals("valid")) {
      nodes = testGraph.buildValidGraph();
    } else if (graph.equals("invalid")) {
      nodes = testGraph.buildInvalidGraph();
    } else {
      throw new IllegalArgumentException(
          "Must specify --graph must be 'valid' or 'invalid'");
    }

    GcsUtil.GcsUtilFactory gcsUtilFactory = new GcsUtil.GcsUtilFactory();
    GcsUtil gcsUtil = gcsUtilFactory.create(options);

    GcsPath gcsOutputPath = GcsPath.fromUri(outputPath);
    WritableByteChannel outChannel = gcsUtil.create(
        gcsOutputPath, "avro/binary");
    OutputStream outStream = Channels.newOutputStream(outChannel);
    AvroFileUtil.writeRecords(outStream, nodes, GraphNodeData.SCHEMA$);
    sLogger.info("Closing the channel");
    if (outChannel.isOpen()) {
      outChannel.close();
    }
    sLogger.info("Done");

    GcsOptions gcsOptions = options.as(GcsOptions.class);
    gcsOptions.getExecutorService().shutdown();
    try {
      gcsOptions.getExecutorService().awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      sLogger.error("Thread was interrupted waiting for execution service to shutdown.");
    }
  }
}
