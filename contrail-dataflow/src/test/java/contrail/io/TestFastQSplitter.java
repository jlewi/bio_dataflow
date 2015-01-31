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
package contrail.io;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.LineReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestFastQSplitter {

  public static final String FastQ_1 =
      "@1/1\n" +
      "GGCGCGGGCCAGTGCGGCAAAGAATTTCGCCGAGATCCCACGCAAGGTGCGCATACCATCACCTACCACCGAGATAATGGCCAGCCGTTCCGTCACTGCC\n" +
      "+\n" +
      "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n" +
      "@2/1\n" +
      "GGCTCCGAAGTGTAGCCCAGTTCTTTTAACTCACGCATTGTCTGTTGCGTGGTTTCATCATCCACGGCTGCATAACCCAGCTCTTTCAGTTGCCAGATTT\n" +
      "+\n" +
      "????????????????????????????????????????????????????????????????????????????????????????????????????\n";

  public static final String FastQ_2 =
          "@@@@@@@@@@@@++++++++\n" +
          "@2/1\n" +
          "GGCTCCGAAGTGTAGCCCAG\n" +
          "+\n" +
          "?????????????????????\n";

  public static final String FastQ_3 =
          "@2/1\n" +
          "GGCTCCGAAGTGTAGCCCAG\n" +
          "+\n" +
          "?????????????????????\n"+
          "@3/1\n" +
          "AAAAACCAGAAAAAAAAAAA\n" +
          "+\n" +
          "?????????????????????\n"+
          "@4/1\n" +
          "AATTACCAGAAAACGTAAAA\n" +
          "+\n" +
          "?????????????????????\n"+
          "@5/1\n" +
          "CATTACCAGAAAACGTAAAA\n" +
          "+\n" +
          "?????????????????????\n"+
          "@8/1\n" +
          "CCTTACCAGAAAACGTAAAA\n" +
          "+\n" +
          "?????????????????????\n";

  private Configuration conf;
  private File tempFile;

  @Before
  public void setup() throws IOException {
    tempFile = File.createTempFile("fqinputformat_test", "fastq");
    conf = new JobConf();
  }

  @After
  public void tearDown() {
    tempFile.delete();
  }

  private void writeToFile(String s) throws IOException {
    PrintWriter pw = new PrintWriter(
        new BufferedWriter(new FileWriter(tempFile)));
    pw.write(s);
    pw.close();
  }

  @Test
  public void testTakeToNextStartFromStart() throws IOException {
    // Test that takeToNextStart works when we start at the start of a file.
    long offset;
    writeToFile(FastQ_1);
    FastQSplitter splitter = new FastQSplitter();

    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    offset = splitter.takeToNextStart(fstream, 0);
    assertEquals(0, offset);
    assertEquals(0, fstream.getPos());
  }

  @Test
  public void testTakeToNextStartFromMiddle() throws IOException {
    // Test that takeToNextStart works when we start in the middle
    // of a split.
    FastQSplitter splitter = new FastQSplitter();

    writeToFile(FastQ_1);

    Configuration conf = new Configuration();
    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    long offset = splitter.takeToNextStart(fstream, 10);
    assertEquals(210, offset);
    assertEquals(210, fstream.getPos());

    // Double check that when we read the next line we get the start of
    // a FastQ Record.
    Text buffer = new Text();
    LineReader lineReader = new LineReader(fstream);
    lineReader.readLine(buffer);
    assertEquals(buffer.toString(), "@2/1");
  }

  @Test
  public void testTakeToNextStartWithInvalidRecord() throws IOException {
    // In this test, we start at a position in the file that contains
    // the "@" symbol and is not a valid line of a FastQ file.
    FastQSplitter splitter = new FastQSplitter();

    writeToFile(FastQ_2);
    Configuration conf = new Configuration();
    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    long offset = splitter.takeToNextStart(fstream, 1);
    assertEquals(21, offset);

    // Double check it by reading the next line.
    LineReader lineReader = new LineReader(fstream);
    Text buffer = new Text();
    lineReader.readLine(buffer);
    assertEquals(buffer.toString(), "@2/1");
  }

  @Test
  public void testRetrieveSplits() throws IOException {
    writeToFile(FastQ_3);
    long streamLength = FastQ_3.length();

    FSDataInputStream fstream =  FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    int splitSize = 60;
    FastQSplitter splitter = new FastQSplitter();

    long bytes = 0;
    for (NumberedFileSplit split:
      splitter.retrieveSplits(
             new Path("dummy"), fstream, streamLength, splitSize)) {
      bytes += split.getLength();
    }

    // Check that the total bytes in all splits put together is equal to the
    // number of bytes in file - that is, no bytes are getting 'missed' in the
    // splitting.
    assertEquals(bytes, streamLength);
  }
}