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
package contrail.io.mapred;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;

import contrail.io.FastQSplitter;
import contrail.io.FastQWritable;
import contrail.io.NumberedFileSplit;

/**
 * InputFormat for FastQFiles.
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
public class FastQInputFormat extends
    FileInputFormat<LongWritable, FastQWritable> implements JobConfigurable {
  private FastQSplitter splitter;
  public static long DEFAULT_SPLIT_SIZE = 100*1000*1000L;

  /**
   * The name of the variable used to configure the split size.
   */
  public final static String SPLIT_SIZE_NAME =  "FastQInputFormat.splitSize";
  public FastQInputFormat() {
    splitter = new FastQSplitter();
  }


  @Override
  public void configure(JobConf arg0) {
    // TODO Auto-generated method stub
  }

  @Override
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
    // TODO(jeremy@lewi.us): Do we really need to use NumberedFileSplit's?
    // The reason for using numbered file split so was that we coud use
    // it to rename reads during error correction but that's not something
    // we are currently using.
    ArrayList<NumberedFileSplit> splits = new ArrayList<NumberedFileSplit>();
    for (FileStatus status : listStatus(job)) {
      for (NumberedFileSplit split : splitter.getSplitsForFile(status, job)) {
        splits.add(split);
      }
    }
    return splits.toArray(new NumberedFileSplit[splits.size()]);
  }
}
