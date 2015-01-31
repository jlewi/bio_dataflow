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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import contrail.io.AvroFileContentsIterator;

/**
 * GraphNodeFilesIterator is a simple wrapper for AvroFileContentsIterator
 * which takes care of casting the data in the files to a GraphNode.
 *
 * Note the GraphNode object returned by next is reused so make a copy
 * if you want to preserve data.
 */
public class GraphNodeFilesIterator
  implements Iterator<GraphNode>, Iterable<GraphNode> {
  private static final Logger sLogger =
      Logger.getLogger(GraphNodeFilesIterator.class);

  private final AvroFileContentsIterator<GraphNodeData> filesIterator;
  private final GraphNode node;
  private final Configuration conf;
  private final ArrayList<Path> files;

  public GraphNodeFilesIterator(Configuration conf, Collection<Path> files) {
    this.files = new ArrayList<Path>();
    this.files.addAll(files);
    this.conf = conf;

    ArrayList<String> stringFiles = new ArrayList<String>();
    for (Path path : this.files) {
      stringFiles.add(path.toString());
    }
    filesIterator = new AvroFileContentsIterator<GraphNodeData>(
        stringFiles, conf);
    node = new GraphNode();
  }

  /**
   * Create the iterator from a glob expression matching the files to use.
   * @return
   */
  public static GraphNodeFilesIterator fromGlob(
      Configuration conf, String glob) {
    // TODO(jeremy@lewi.us): We should check if the input path is a directory
    // and if it is we should use its contents.
    Path inputPath = new Path(glob);

    FileStatus[] fileStates = null;
    try {
      FileSystem fs = inputPath.getFileSystem(conf);
      fileStates = fs.globStatus(inputPath);
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

    return new GraphNodeFilesIterator(conf, inputFiles);
  }

  @Override
  public Iterator<GraphNode> iterator() {
    return new GraphNodeFilesIterator(conf, files);
  }

  @Override
  public boolean hasNext() {
    return filesIterator.hasNext();
  }

  @Override
  public GraphNode next() {
    node.setData(filesIterator.next());
    return node;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
