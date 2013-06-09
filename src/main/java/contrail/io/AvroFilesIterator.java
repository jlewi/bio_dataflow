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
package contrail.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

/**
 * AvroFilesIterator provides an iterator for iterating over all the data
 * in a set of avro files.
 */
public class AvroFilesIterator<T> implements Iterator<T>, Iterable<T> {
  private ArrayList<Path> files;
  private Configuration conf;

  private Iterator<Path> fileIterator;
  private Iterator<T> dataIterator;
  private boolean hasNextData;
  private FSDataInputStream inStream;
  public AvroFilesIterator(Configuration conf, Collection<Path> files) {
    this.files = new ArrayList<Path>();
    this.files.addAll(files);
    this.conf = conf;

    fileIterator = files.iterator();

    hasNextData = false;
    loadNextFile();
  }

  /*
   * Keep loading files until we find one which has nodes
   */
  private void loadNextFile() {
    hasNextData = false;
    dataIterator = null;

    if (inStream != null) {
      try {
        inStream.close();
      } catch (IOException e) {
        throw new RuntimeException(
            "There was a problem closing the avro file." +
            " Exception:" + e.getMessage());
      }
    }

    while (fileIterator.hasNext()) {
      Path path = fileIterator.next();
      try {
        FSDataInputStream inStream = path.getFileSystem(conf).open(path);
        SpecificDatumReader<T> reader =
            new SpecificDatumReader<T>();
        DataFileStream<T> avroStream =
            new DataFileStream<T>(inStream, reader);
        dataIterator = avroStream.iterator();
        if (dataIterator.hasNext()) {
          break;
        } else {
          dataIterator = null;
        }
      } catch (IOException exception) {
        throw new RuntimeException(
            "There was a problem reading the avro file." +
                " Exception:" + exception.getMessage());
      }
    }
    if (dataIterator == null) {
      hasNextData = false;
    } else {
      hasNextData = dataIterator.hasNext();
    }
  }
  @Override
  public boolean hasNext() {
    return hasNextData;
  }


  @Override
  public T next() {
    if (!hasNextData) {
      throw new NoSuchElementException();
    }
    T result = dataIterator.next();

    if (!dataIterator.hasNext()) {
      loadNextFile();
    }

    return result;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterator<T> iterator() {
    return new AvroFilesIterator<T>(conf, files);
  }
}
