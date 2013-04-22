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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import contrail.io.AvroFilesIterator;

/**
 * GraphNodeFilesIterator is a simple wrapper for AvroFilesIterator
 * which takes care of casting the data in the files to a GraphNode.
 *
 * Note the GraphNode object returned by next is reused so make a copy
 * if you want to preserve data.
 */
public class GraphNodeFilesIterator
  implements Iterator<GraphNode>, Iterable<GraphNode> {

  private AvroFilesIterator<GraphNodeData> filesIterator;
  private GraphNode node;
  private Configuration conf;
  private ArrayList<Path> files;

  public GraphNodeFilesIterator(Configuration conf, Collection<Path> files) {
    this.files = new ArrayList<Path>();
    this.files.addAll(files);
    this.conf = conf;
    filesIterator = new AvroFilesIterator<GraphNodeData>(conf, files);
    node = new GraphNode();
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
