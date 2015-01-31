package contrail.io.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import contrail.io.FastQSplitter;
import contrail.io.FastQWritable;
import contrail.io.NumberedFileSplit;

/**
 * InputFormat for FastQFiles using the new mapreduce API.
 *
 * To split FastQ files we start by evenly dividing the file into partitions of
 * size splitSize. Since the boundaries won't correspond to FastQ records,
 * we seek from the start of the partition until we find the start of a
 * FastQRecord. That position is then return as the start of the split.
 * Similarly, we will read past the end of the partition to get a complete
 * FastQ record.
 *
 * In general, the algorithm is to seek
 * ahead in jumps of the splitSize, and then move ahead a few bytes to find
 * the record boundary. For the FastQ File Format,
 * see wiki - http://en.wikipedia.org/wiki/FASTQ_format
 */
public class FastQInputFormatNew extends
    FileInputFormat<LongWritable, FastQWritable> {
  FastQSplitter splitter;
  public FastQInputFormatNew() {
    splitter = new FastQSplitter();
  }

  @Override
  public RecordReader<LongWritable, FastQWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    // The framework will call RecordReader.initialize(
    // InputSplit, TaskAttemptContext) before the reader is used.
    return new FastQRecordReader();
  }

  /**
   * Logically splits the set of input files for the job, roughly at
   * size defined by splitSize.
   *
   * @see org.apache.hadoop.mapred.FileInputFormat#getSplits(JobConf, int)
   */
  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException {
    ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
    for (FileStatus status : listStatus(context)) {
      for (NumberedFileSplit split : splitter.getSplitsForFile(
          status, context.getConfiguration())) {
        splits.add(split);
      }
    }
    return splits;
  }
}
