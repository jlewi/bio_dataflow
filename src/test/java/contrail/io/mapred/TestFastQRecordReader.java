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
package contrail.io.mapred;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;
import static org.junit.Assert.*;

import contrail.io.FastQWritable;
import contrail.io.NumberedFileSplit;
import contrail.io.mapred.FastQRecordReader;
import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.util.FileHelper;

public class TestFastQRecordReader {
  private static class TestCase {
    public String fileName;
    public ArrayList<FastQRecord> records;
    public long fileSize;
    public TestCase() {
      records = new ArrayList<FastQRecord>();
    }
  }

  private TestCase createTest() {
    TestCase testCase = new TestCase();
    Alphabet alphabet = DNAAlphabetFactory.create();

    Random generator = new Random();
    int length = 20;

    File tempDir = FileHelper.createLocalTempDir();
    File fastqFile = new File(tempDir, "input.fastq");
    PrintStream outStream = null;

    try {
      outStream = new PrintStream(fastqFile);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    for (int i=0; i < 1; ++i) {
      FastQRecord record = new FastQRecord();
      record.setId(String.format("Record%d/1", i));
      record.setRead(AlphabetUtil.randomString(generator, length, alphabet));
      record.setQvalue("!!!@#");
      FastUtil.writeFastQRecord(outStream, record);

      testCase.records.add(record);
    }
    outStream.close();

    testCase.fileName = fastqFile.toString();
    testCase.fileSize = fastqFile.length();
    return testCase;
  }



  @Test
  public void testReader() throws IOException {
    TestCase testCase = createTest();
    Configuration conf = new Configuration();
    NumberedFileSplit split = new NumberedFileSplit(
        new Path(testCase.fileName), 0, testCase.fileSize, 0, new String[]{});

    FastQRecordReader reader = new FastQRecordReader(
        conf, split);

    FastQWritable record = new FastQWritable();
    LongWritable key = new LongWritable();

    ArrayList<FastQRecord> read = new ArrayList<FastQRecord>();
    while(reader.next(key, record)) {
      FastQRecord avroRecord = new FastQRecord();
      avroRecord.setId(record.getId());
      avroRecord.setRead(record.getDNA());
      avroRecord.setQvalue(record.getQValue());
      read.add(avroRecord);
    }
    assertEquals(testCase.records, read);
  }
}
