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
package contrail.util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class iterates over the contents in a series of avro files.
 *
 * This class provides a unified view of all the records stored in a set of
 * avro files. The iterator iterates over all the items in the first file,
 * then all the items in the second file and so on until all items have been
 * returned.
 *
 * @param <T>: The record type for the records we are iterating over.
 */
public class AvroFileContentsIterator<T> implements Iterator<T>{
  private Configuration conf;

  // The list of files.
  private List<String> files;

  // The iterator for the current file.
  private Iterator<T> currentIterator;

  // Iterator over the files.
  private Iterator<String> fileIterator;

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

    currentIterator = openFile(fileIterator.next());
    hasMoreRecords = null;
  }

  public boolean hasNext() {
    if (hasMoreRecords == null) {
      // Need to recompute whether there are more records.
      if (currentIterator.hasNext()) {
        hasMoreRecords = true;
        return hasMoreRecords;
      }
      // Loop over the remaining files until we either find a file with
      // records to process or we run out of records to process.
      while (fileIterator.hasNext()) {
        currentIterator = openFile(fileIterator.next());
        if (currentIterator.hasNext()) {
          hasMoreRecords = true;
          return hasMoreRecords;
        }
      }

      // If we reached this point then there are no more records.
      hasMoreRecords = false;
    }

    return hasMoreRecords;
  }

  public T next() {
    // Reset hasMoreRecords so that it will be recomputed.
    hasMoreRecords = null;
    return currentIterator.next();
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

  public void remove() {
    throw new UnsupportedOperationException();
  }
}
