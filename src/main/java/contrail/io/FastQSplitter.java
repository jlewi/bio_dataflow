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
package contrail.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

/**
 * Helper class for splitting FastQFiles.
 *
 *  * To split FastQ files we start by evenly dividing the file into partitions of
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
 *
 * This is a separate file from FastQInputFormat and FastQInputFormatNew so
 * that we can support both the old and new mapreduce apis.
 */
public class FastQSplitter {
  private static final Logger sLogger =
      Logger.getLogger(FastQSplitter.class);

  // Desired size of the split The split is not guaranteed to be exactly this
  // size. It may be a few bytes more.
  private final Text buffer;

  public static long DEFAULT_SPLIT_SIZE = 100*1000*1000L;

  /**
   * The name of the variable used to configure the split size.
   */
  public final static String SPLIT_SIZE_NAME = "FastQInputFormat.splitSize";

  public FastQSplitter() {
    buffer = new Text();
  }

  public List<NumberedFileSplit> getSplitsForFile(FileStatus status,
      Configuration conf) throws IOException {
    long splitSize = conf.getLong(SPLIT_SIZE_NAME, DEFAULT_SPLIT_SIZE);
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
    return retrieveSplits(fileName, in, fileSize, splitSize);
  }

  public List<NumberedFileSplit> retrieveSplits(
      Path fileName, FSDataInputStream stream, long fileSize, long splitSize)
          throws IOException {
    List<NumberedFileSplit> splits = new ArrayList<NumberedFileSplit>();
    long bytesConsumed = 0;
    long begin = bytesConsumed;
    int splitNumber = 1;

    if (splitSize <= 0) {
      throw new RuntimeException("splitSize must be greater than 0.");
    }

    // cast as bytes because LineReader takes a byte array.
    takeToNextStart(stream, 0);
    while (bytesConsumed < fileSize) {
      // We allow the last split to be 1.5 times the normal splitSize.
      if (bytesConsumed + (1.5 * splitSize) <= fileSize) {
        begin = bytesConsumed;
        // jump by the length of the split.
        bytesConsumed += splitSize;

        bytesConsumed = takeToNextStart(stream, bytesConsumed);

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
    sLogger.info(
        fileName.toString() + " resulted in " + splits.size() +  " splits ");
    return splits;
  }

  /**
   * Advance the stream to first FastQRecord at or after position start.
   *
   * @param stream: The stream.
   * @param start: The position to start searching for the start of a
   *   fastqrecord
   * @return: The position of the stream.
   * @throws IOException
   *
   * If a FastQRecord begins at position start then the stream will point
   * to that record.
   */
  public long takeToNextStart(
      FSDataInputStream stream, long start) throws IOException {
    stream.seek(start);
    LineReader reader = new LineReader(stream);

    // We need to keep track of the actual number of bytes read in order
    // to compute the offset. We can't simply use stream.getPos() because
    // we are using a buffered reader so the reader could have read past
    // the end of the line.
    int bytesRead = 0;

    // Records the start position of the FastQ record.
    long recordStart = start;
    boolean foundRecord = false;

    // We use recordLength to record the number of bytes read since the
    // last position elgible to be the start of the record.
    long recordLength = 0;
    do {
      recordStart += recordLength;
      recordLength = 0;

      bytesRead = reader.readLine(buffer);

      // The first line of a FastQ record begins with a '@' but
      // '@' can also be a quality score.
      if (bytesRead > 0 && buffer.getBytes()[0] != '@') {
        recordLength += bytesRead;
        continue;
      }

      // We keep track of the length of the first line because if
      // this turns out to not be a valid record, then we will want to continue
      // reading at recordStart + lengthFirstLine.
      long lengthFirstLine = bytesRead;

      // read two lines.
      bytesRead = reader.readLine(buffer);
      recordLength += bytesRead;
      bytesRead = reader.readLine(buffer);
      recordLength += bytesRead;
      if (bytesRead > 0 && buffer.getBytes()[0] == '+') {
        foundRecord = true;
        break;
      }
      // This turned out to not be a record so we need to go the end of the
      // line that contained '@'.
      stream.seek(recordStart + lengthFirstLine);
      recordLength = lengthFirstLine;

      // We need to create a new lineReader because we want to continue
      // reading from the position we just seeked to in the file.
      reader = new LineReader(stream);
    } while(bytesRead > 0);

    if (!foundRecord) {
      recordStart = -1;
    }
    stream.seek(recordStart);
    return recordStart;
  }
}
