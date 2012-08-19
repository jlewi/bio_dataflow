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
package contrail.graph;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Some miscellaneous utilities for working with graphs.
 */
public class GraphUtil {
  /**
   * Order strings lexicographically and return the largest id.
   *
   * @param id1
   * @param id2
   * @return: The largest id.
   */
  public static CharSequence computeMajorID(
      CharSequence id1, CharSequence id2) {
    CharSequence major = "";
    if (id1.toString().compareTo(id2.toString()) > 0) {
      major = id1;
    } else  {
      major = id2;
    }
    return major;
  }

  /**
   * Order strings lexicographically and return the smallest id.
   *
   * @param id1
   * @param id2
   * @return: The smaller id.
   */
  public static CharSequence computeMinorID(
      CharSequence id1, CharSequence id2) {
    CharSequence minor = "";
    if (id1.toString().compareTo(id2.toString()) > 0) {
      minor = id2;
    } else  {
      minor = id1;
    }
    return minor;
  }

  /**
   * Write a list of a graph nodes to an avro.
   * @param avroFile
   * @param nodes
   */
  public static void writeGraphToFile(
      File avroFile, Collection<GraphNode> nodes) {
    // Write the data to the file.
    Schema schema = (new GraphNodeData()).getSchema();
    DatumWriter<GraphNodeData> datumWriter =
        new SpecificDatumWriter<GraphNodeData>(schema);
    DataFileWriter<GraphNodeData> writer =
        new DataFileWriter<GraphNodeData>(datumWriter);

    try {
      writer.create(schema, avroFile);
      for (GraphNode node: nodes) {
        writer.append(node.getData());
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. " +
           "Exception: " + exception.getMessage());
    }
  }
}
