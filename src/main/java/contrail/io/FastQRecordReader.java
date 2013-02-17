package contrail.io;
import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Record reader for FastQ files.
 * This record reader reads a 4 line fastq record
 * and passes it to the mapper as FastQWritable which
 * is a subclass of the Hadoop Text type.
 */
class FastQRecordReader implements
    RecordReader<LongWritable, FastQWritable> {
  //The constructor gets this as a parameter. This data member stores it in a
  //private member so it can be used to get the number in the getSplitsForFile
  //method.
  private long               start;
  private long               end;
  private FSDataInputStream  fileIn;

  public FastQRecordReader(Configuration job, NumberedFileSplit split)
      throws IOException {
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    // Open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    fileIn.seek(start);
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public FastQWritable createValue() {
    return new FastQWritable();
  }

  @Override
  public synchronized boolean next(LongWritable key, FastQWritable value)
      throws IOException {
    try {
      value.readFields(fileIn);
    } catch (EOFException e) {
      return false;
    }
    return true;
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      long pos = start;
      try {
        pos = getPos();
      } catch (IOException e) {
        // Do nothing..
      }
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }

  public long getPos() throws IOException {
    // TODO(jeremy@lewi.us): Does pos return the correct possition even
    // if we buffered the input?
    return fileIn.getPos();
  }

  public void close() throws IOException {
    fileIn.close();
  }
}
