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
import java.util.Iterator;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import contrail.util.ContrailLogger;

/**
 * This class iterates over the contents in a series of avro files.
 *
 * This class provides a unified view of all the records stored in a set of
 * avro files. The iterator iterates over all the items in the first file,
 * then all the items in the second file and so on until all items have been
 * returned.
 *
 * TODO(jeremy@lewi.us): We can probably just use
 * org.apache.commons.iterators.IteratorChain
 *
 * TODO(jeremy@lewi.us): Add a close method.
 * @param <T>: The record type for the records we are iterating over.
 */
public class AvroFileContentsIterator<T> implements Iterator<T>, Iterable<T> {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(AvroFileContentsIterator.class);

  private final Configuration conf;

  // The list of files.
  private final List<String> files;

  // The iterator for the current file.
  private IteratorStreamTuple current;

  // Iterator over the files.
  private final Iterator<String> fileIterator;

  // Keep track of whether we have more values.
  private Boolean hasMoreRecords;

  /**
   * Construct the iterator.
   * @param files: List of files to iterate over. These can either be on the
   *   local filesystem or an HDFS.
   * @param conf: The configuration used to resolve file paths and construct
   *   the filesystem.
   */
  public AvroFileContentsIterator (List<String> files, Configuration conf) {
    this.files = files;
    this.conf = conf;
    fileIterator = this.files.iterator();

    if (!files.isEmpty()) {
      current = openFile(fileIterator.next());
      hasMoreRecords = null;
    } else {
      current = null;
      hasMoreRecords = false;
    }
  }

  /**
   * Create the iterator from a glob expression matching the files to use.
   * @return
   */
  public static <T> AvroFileContentsIterator<T> fromGlob(
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

    ArrayList<String> inputFiles = new ArrayList<String>();

    for (FileStatus status : fileStates) {
     if (status.isDir()) {
       sLogger.info("Skipping directory:" + status.getPath());
         continue;
      }
      inputFiles.add(status.getPath().toString());
    }
    sLogger.info("Number of input files: " + inputFiles.size());

    return new AvroFileContentsIterator<T>(inputFiles, conf);
  }

  @Override
  public boolean hasNext() {
    if (hasMoreRecords == null) {
      if (current == null) {
        hasMoreRecords = false;
        return hasMoreRecords;
      }
      // Need to recompute whether there are more records.
      if (current.avroStream.hasNext()) {
        hasMoreRecords = true;
        return hasMoreRecords;
      }
      try {
        current.close();
      } catch (IOException e) {
        sLogger.warn("Exception occurred while closing the stream: " +
                     e.getMessage());
      }

      // Loop over the remaining files until we either find a file with
      // records to process or we run out of records to process.
      while (fileIterator.hasNext()) {
        current = openFile(fileIterator.next());
        if (current.avroStream != null && current.avroStream.hasNext()) {
          hasMoreRecords = true;
          return hasMoreRecords;
        }
        try {
          current.close();
        } catch (IOException e) {
          sLogger.warn("Exception occurred while closing the stream: " +
                       e.getMessage());
        }
      }

      // If we reached this point then there are no more records.
      hasMoreRecords = false;
    }

    return hasMoreRecords;
  }

  @Override
  public T next() {
    // Reset hasMoreRecords so that it will be recomputed.
    hasMoreRecords = null;
    return current.avroStream.next();
  }

  private class IteratorStreamTuple {
    public DataFileStream<T> avroStream;
    public FSDataInputStream inStream;

    public void close() throws IOException {
      if (avroStream != null) {
        avroStream.close();
        avroStream = null;
      }
      if (inStream != null) {
        inStream.close();
      }
    }
  }

  /**
   * Construct an iterator for the given file.
   * @param path
   * @return
   */
  private IteratorStreamTuple openFile(String fileName) {
    IteratorStreamTuple result = new IteratorStreamTuple();

    try {
      Path path = new Path(fileName);
      FileSystem fs = path.getFileSystem(conf);
      result.inStream = fs.open(path);
      SpecificDatumReader<T> reader = new SpecificDatumReader<T>();
      result.avroStream = new DataFileStream<T>(result.inStream, reader);
    } catch (IOException exception) {
      throw new RuntimeException(
          "There was a problem reading the avro file: " + fileName + " " +
              "Exception:" + exception.getMessage());
    }
    return result;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    return new AvroFileContentsIterator<T>(files, conf);
  }
}
