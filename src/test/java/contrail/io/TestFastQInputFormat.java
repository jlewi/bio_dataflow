package contrail.io;

import java.io.File;
import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class TestFastQInputFormat
{

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
    FastQInputFormat inFormat = new FastQInputFormat();

    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    offset = inFormat.takeToNextStart(fstream, 0);
    assertEquals(0, offset);
    assertEquals(0, fstream.getPos());
  }

  @Test
  public void testTakeToNextStartFromMiddle() throws IOException {
    // Test that takeToNextStart works when we start in the middle
    // of a split.
    FastQInputFormat inFormat = new FastQInputFormat();

    writeToFile(FastQ_1);

    Configuration conf = new Configuration();
    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    long offset = inFormat.takeToNextStart(fstream, 10);
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
    FastQInputFormat inFormat = new FastQInputFormat();

    writeToFile(FastQ_2);
    Configuration conf = new Configuration();
    FSDataInputStream fstream = FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    long offset = inFormat.takeToNextStart(fstream, 1);
    assertEquals(21, offset);

    // Double check it by reading the next line.
    LineReader lineReader = new LineReader(fstream);
    Text buffer = new Text();
    lineReader.readLine(buffer);
    assertEquals(buffer.toString(), "@2/1");
  }


  @Test
  public void testRetrieveSplits() throws IOException {
    //List<NumberedFileSplit> numberedSplitList;
    writeToFile(FastQ_3);
    long streamLength = FastQ_3.length();

    //Text buffer = new Text();
    //FastQInputFormat inFormat = new FastQInputFormat();
    //ByteBuffer byteBuffer = ByteBuffer.wrap(ByteUtil.stringToBytes(FastQ_3));
    //MockFSDataInputStream mock_stream = new MockFSDataInputStream(byteBuffer);


    FSDataInputStream fstream =  FileSystem.get(conf).open(
        new Path(tempFile.getPath()));

    JobConf conf = new JobConf();
    conf.setInt("splitSize",60);
    FastQInputFormat fastQInputFormat = new FastQInputFormat();
    fastQInputFormat.configure(conf);

    long bytes = 0;
    for (NumberedFileSplit split: fastQInputFormat.retrieveSplits(new Path("dummy"), fstream, streamLength))
      bytes += split.getLength();

    // Check that the total bytes in all splits put together is equal to the
    // number of bytes in file - that is, no bytes are getting 'missed' in the splitting.
    assertEquals(bytes, streamLength);
  }

//  public static void main(String args[]) {
//    org.junit.runner.JUnitCore.main(TestFastQInputFormat.class.getName());
//  }
}