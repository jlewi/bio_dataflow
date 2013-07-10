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
package contrail.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import contrail.util.ContrailLogger;

/**
 * This class iterates over the contents in a series of sorted avro files.
 *
 * Within each file the records should be sorted according to the comparator.
 * The iterator iterates over the files effectively performing a merge sort
 * in order to return the items in sorted order across all files.
 * @param <T>: The record type for the records we are iterating over.
 */
public class CollatingAvroFileContentsIterator<T>
    implements Iterator<T>, Iterable<T> {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(AvroFileContentsIterator.class);

  private final Configuration conf;

  // The list of files.
  private final List<String> files;

  ArrayList<FSDataInputStream> streams;

  private final CollatingIterator iterator;

  private final Comparator<T> comparator;
  /**
   * Construct the iterator.
   * @param files: List of files to iterate over. These can either be on the
   *   local filesystem or an HDFS.
   * @param conf: The configuration used to resolve file paths and construct
   *   the filesystem.
   */
  public CollatingAvroFileContentsIterator (
      List<String> files, Configuration conf, Comparator<T> comparator) {
    this.files = files;
    this.conf = conf;

    iterator = new CollatingIterator(comparator);

    for (String file : files) {
      iterator.addIterator(openFile(file));
    }
    this.comparator = comparator;
  }

  /**
   * Create the iterator from a glob expression matching the files to use.
   * @return
   */
  public static <T> CollatingAvroFileContentsIterator<T> fromGlob(
      String glob, Configuration conf, Comparator<T> comparator) {
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

    ArrayList<String> inputFiles = new ArrayList<String>();

    for (FileStatus status : fileStates) {
     if (status.isDir()) {
       sLogger.info("Skipping directory:" + status.getPath());
         continue;
      }
      sLogger.info("Input file:" + status.getPath()) ;
      inputFiles.add(status.getPath().toString());
    }

    return new CollatingAvroFileContentsIterator<T>(inputFiles, conf, comparator);
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return (T) iterator.next();
  }

  /**
   * Construct an iterator for the given file.
   * @param path
   * @return
   */
  private Iterator<T> openFile(String path) {
    DataFileStream<T> avroStream = null;

    FileSystem fs = null;
    try{
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    try {
      FSDataInputStream inStream = fs.open(new Path(path));
      SpecificDatumReader<T> reader = new SpecificDatumReader<T>();
      avroStream = new DataFileStream<T>(inStream, reader);
    } catch (IOException exception) {
      throw new RuntimeException(
          "There was a problem reading the avro file: " + path + " " +
              "Exception:" + exception.getMessage());
    }
    return avroStream;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    return new CollatingAvroFileContentsIterator<T>(files, conf, comparator);
  }
}
