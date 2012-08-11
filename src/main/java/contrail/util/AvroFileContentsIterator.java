package contrail.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;

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
  // The list of files.
  private List<String> files;

  // The iterator for the current file.
  private Iterator<T> currentIterator;

  // Iterator over the files.
  private Iterator<String> fileIterator;

  // Keep track of whether we have more values.
  Boolean hasMoreRecords;

  public AvroFileContentsIterator (List<String> files) {
    this.files = files;
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

    try {
      FileInputStream inStream = new FileInputStream(path);
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
