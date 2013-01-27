package contrail.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

//import org.apache.hadoop.mapred.LineRecordReader.LineReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import contrail.util.MockFSDataInputStream;

/** InputFormat to read FastQFiles.
 *
 * In general, the algorithm is to seek
 * ahead in jumps of the splitSize, and then move ahead a few bytes to find
 * the record boundary. For the FastQ File Format,
 * see wiki - http://en.wikipedia.org/wiki/FASTQ_format
 */

@SuppressWarnings("deprecation")
public class FastQInputFormat extends
FileInputFormat<LongWritable, FastQText> implements JobConfigurable {

  // TODO(deepak): according to the fastq spec, is this the maximum?
  private static final int MAX_READ_LENGTH = 500;

  private static final Logger sLogger
  = Logger.getLogger(FastQInputFormat.class);

  // Desired size of the split The split is not guaranteed to be exactly this
  // size. It may be a few bytes more.

  public long splitSize;

  private long end;
  // to keep track of the current offset
  private long currentOffset;

  private Text buffer = new Text();

  @Override
  public void configure(JobConf job) {
    splitSize = job.getLong("FastQInputFormat.splitSize",100*1000*1000);
  }

  public FastQRecordReader getRecordReader(InputSplit genericSplit,
      JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(genericSplit.toString());
    return new FastQRecordReader(job, (NumberedFileSplit) genericSplit);
  }

  /**
   * Logically splits the set of input files for the job, roughly at
   * size defined by splitSize.
   *
   * @see org.apache.hadoop.mapred.FileInputFormat#getSplits(JobConf, int)
   */

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits)
      throws IOException {
    ArrayList<NumberedFileSplit> splits = new ArrayList<NumberedFileSplit>();
    for (FileStatus status : listStatus(job)) {
      for (NumberedFileSplit split : getSplitsForFile(status, job)) {
        splits.add(split);
      }
    }
    return splits.toArray(new NumberedFileSplit[splits.size()]);
  }

  // Given the input data stream, and an offset, take us to the very next start of new record.
  public long takeToNextStart(FSDataInputStream stream, long start, long end) throws IOException
  {

    if (start > 0)
    {
      stream.seek(start);
      LineReader reader = new LineReader(stream);

      int bytesRead = 0;
      do
      {
        bytesRead = reader.readLine(buffer, (int) Math.min((end - start),MAX_READ_LENGTH) );
        if (bytesRead > 0 && buffer.getBytes()[0] != '@')
          start += bytesRead;
        else
        {
          long endOfCurrentLine = start + bytesRead;
          // read two lines.
          bytesRead = reader.readLine(buffer, (int) Math.min((end - start),MAX_READ_LENGTH) );
          bytesRead = reader.readLine(buffer, (int) Math.min((end - start),MAX_READ_LENGTH) );
          if (bytesRead > 0 && buffer.getBytes()[0] == '+')
            break;
          else
          {
            start = endOfCurrentLine;
            stream.seek(start);
            reader = new LineReader(stream);
          }
        }
      }while(bytesRead > 0);

      // control comes here from break.
      stream.seek(start);
    }
    return start;
  }

  @SuppressWarnings("deprecation")
  public List<NumberedFileSplit> getSplitsForFile(FileStatus status,
      Configuration conf) throws IOException {

    Path fileName = status.getPath();

    if (status.isDir()) {
      throw new IOException("Not a file: " + fileName);
    }
    FileSystem fs = fileName.getFileSystem(conf);

    long fileSize = fs.getFileStatus(fileName).getLen();
    // open the input stream
    FSDataInputStream in = fs.open(fileName);

    //abstracting the splitting functionality as another method
    // to help ease testing.
    return retrieveSplits(fileName, in,fileSize);
  }

  public List<NumberedFileSplit> retrieveSplits(Path fileName, FSDataInputStream stream, long fileSize) throws IOException
  {
    List<NumberedFileSplit> splits = new ArrayList<NumberedFileSplit>();
    long bytesConsumed = 0;
    long length = 0;
    long begin = bytesConsumed;
    int splitNumber = 1;

    int counter = 0;

    // cast as bytes because LineReader takes a byte array.
    takeToNextStart(stream, 0, fileSize);
    while (bytesConsumed < fileSize) {
      // We allow the last split to be 1.5 times the normal splitSize.
      if (bytesConsumed + (1.5 * splitSize) <= fileSize) {
        begin = bytesConsumed;
        // jump by the length of the split.
        bytesConsumed += splitSize;

        bytesConsumed = takeToNextStart(stream, bytesConsumed, fileSize);

        splits.add(new NumberedFileSplit(fileName, begin,
            bytesConsumed - begin, splitNumber, new String[] {}));
        splitNumber++;

      } else {
        // last few bytes remaining - create a new split.
        begin = bytesConsumed;
        bytesConsumed = fileSize;
        splits.add(new NumberedFileSplit(fileName, begin,
            (bytesConsumed - begin), splitNumber, new String[] {}));
        splitNumber++;

        stream.close();
        break;
      }
    }
    sLogger.info(fileName.toString()+" resulted in "+splits.size()+" splits ");
    return splits;
  }

  public static boolean isRecord(char ch) {
    return (ch == '+' ? true : false);
  }
}
