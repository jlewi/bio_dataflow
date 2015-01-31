package contrail.io.mapreduce;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import contrail.io.FastQWritable;
import contrail.io.NumberedFileSplit;

/**
 * Record reader for FastQ files using the new mapreduce API.
 *
 * This record reader reads a 4 line fastq record
 * and passes it to the mapper as FastQWritable which
 * is a subclass of the Hadoop Text type.
 *
 */
class FastQRecordReader extends RecordReader<LongWritable, FastQWritable> {
  //The constructor gets this as a parameter. This data member stores it in a
  //private member so it can be used to get the number in the getSplitsForFile
  //method.
  private long               start;
  private long               end;
  private FSDataInputStream  fileIn;

  private LongWritable key;
  private FastQWritable value;

  public FastQRecordReader() {
    key = new LongWritable();
    value = new FastQWritable();
  }

  /**
   * Get the progress within the split
   */
  @Override
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
    // TODO(jeremy@lewi.us): Does pos return the correct position even
    // if we buffered the input?
    return fileIn.getPos();
  }

  @Override
  public void close() throws IOException {
    fileIn.close();
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public FastQWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext context)
      throws IOException, InterruptedException {
    if (!(inputSplit instanceof NumberedFileSplit)) {
      throw new RuntimeException(
          "Input split isn't type NumberedSplit. Class is: " +
              inputSplit.getClass().getName());
    }
    NumberedFileSplit split = (NumberedFileSplit) inputSplit;
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();

    // Open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(context.getConfiguration());
    fileIn = fs.open(file);
    fileIn.seek(start);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Check if we are at the end of the split.
    if (getPos() >= end) {
      return false;
    }
    try {
      value.readFields(fileIn);
    } catch (EOFException e) {
      return false;
    }
    return true;
  }
}
