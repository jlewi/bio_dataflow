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
package contrail.stages;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.avro.specific.SpecificData;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.OutputCollectorMock;
import contrail.ReporterMock;
import contrail.io.FastQWritable;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAAlphabetWithNFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.sequences.Sequence;
import contrail.util.FileHelper;

public class TestReverseReads {
  /**
   * Generate a random q value of the specified length.
   * @param generator
   * @param length
   * @return
   */
  private String randomQValue(Random generator, int length) {
    byte[] letters = new byte[length];
    for (int i = 0; i < length; ++i) {
      // qValues are encoded using ascii values between 33 and 126
      letters[i] = (byte)(33 + generator.nextInt(93));
    }

    String qvalue = null;

    try {
      qvalue = new String(letters, "US-ASCII");
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      fail("Could not create qvalue");
    }

    return qvalue;
  }

  private class TestCase {
    public ArrayList<FastQWritable> input;
    public ArrayList<FastQWritable> expected;

    public TestCase() {
      input = new ArrayList<FastQWritable>();
      expected = new ArrayList<FastQWritable>();
    }
  }

  @Test
  public void test() {
    Random generator = new Random();

    TestCase cases = new TestCase();

    for (int i = 0; i < 10; ++i) {
      FastQWritable record = new FastQWritable();
      int length = 37;
      String id = String.format("record_%d", i);
      String read =
          AlphabetUtil.randomString(
              generator, length, DNAAlphabetWithNFactory.create());
      String qvalue = randomQValue(generator, length);

      record.setId(id);
      record.setDNA(read);
      record.setQValue(qvalue);

      cases.input.add(record);

      FastQWritable expected = new FastQWritable();

      Sequence sequence = new Sequence(read, DNAAlphabetWithNFactory.create());
      String reversedRead = DNAUtil.reverseComplement(sequence).toString();
      String reversedQValue = StringUtils.reverse(qvalue);
      expected.setId(id);
      expected.setDNA(reversedRead);
      expected.setQValue(reversedQValue);

      cases.expected.add(expected);
    }

    OutputCollectorMock<FastQWritable, NullWritable> collector =
        new OutputCollectorMock<FastQWritable, NullWritable>(
            FastQWritable.class, NullWritable.class);
    ReporterMock reporter = new ReporterMock();
    ReverseReads.ReverseMapper mapper = new ReverseReads.ReverseMapper();
    mapper.configure(new JobConf());
    for (FastQWritable input : cases.input) {
      try {
        mapper.map(
            new LongWritable(0), input, collector, reporter);
      } catch (IOException e) {
        e.printStackTrace();
        fail("Mapper failed.");
      }
    }

    // Check the output.
    assertEquals(cases.expected.size(), collector.outputs.size());

    for (int i = 0; i < cases.expected.size(); ++i) {
      FastQWritable actual = collector.outputs.get(i).key;
      FastQWritable expected = cases.expected.get(i);

      assertEquals(expected.getId(), actual.getId());
      assertEquals(expected.getDNA(), actual.getDNA());
      assertEquals(expected.getQValue(), actual.getQValue());
    }
  }

  @Test
  public void testRun() {
    Random generator = new Random();
    int numFiles = 1;
    int numReads = 100;
    int readLength = 2000;

    File tempDir = FileHelper.createLocalTempDir();
    String outputPath = FilenameUtils.concat(tempDir.getPath(), "output");

    HashMap<String, FastQRecord> reads = new HashMap<String, FastQRecord>();

    for (int fileIndex = 0; fileIndex < numFiles; ++fileIndex) {
      File fastqFile = new File(
          tempDir, String.format("reads_%d.fq", fileIndex));
      PrintStream outStream = null;
      try {
        outStream = new PrintStream(fastqFile);
      } catch (IOException exception) {
        fail("Could not open output stream:" + exception.getMessage());
      }

      for (int i = 0; i < numReads; ++i) {
        FastQRecord read = new FastQRecord();
        read.setId(String.format("read_%d_%d", fileIndex, i));
        read.setRead(AlphabetUtil.randomString(
            generator, readLength, DNAAlphabetFactory.create()));
        read.setQvalue(AlphabetUtil.randomString(
            generator, readLength, DNAAlphabetFactory.create()));

        FastUtil.writeFastQRecord(outStream, read);

        // Reverse the read.
        FastQRecord reversed = new FastQRecord();
        Sequence sequence = new Sequence(
            read.getRead().toString(), DNAAlphabetFactory.create());
        reversed.setRead(DNAUtil.reverseComplement(sequence).toString());
        reversed.setId(read.getId().toString());
        reversed.setQvalue(StringUtils.reverse(read.getQvalue().toString()));
        reads.put(read.getId().toString(), reversed);
      }

      outStream.close();
    }

    // Run it.
    ReverseReads stage = new ReverseReads();
    HashMap<String, Object> stageOptions = new HashMap<String, Object>();
    stageOptions.put("inputpath", tempDir.toURI().toString());
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);

    if (!stage.execute()) {
      fail("ReverseReads failed.");
    }

    long numInputs = stage.getNumMapInputRecords();
    long numOutputs = stage.getCounter(
        "org.apache.hadoop.mapred.Task$Counter",
        "MAP_OUTPUT_RECORDS");

    assertEquals(numReads * numFiles, numInputs);
    assertEquals(numReads * numFiles, numOutputs);

    ArrayList<String> outFiles = FileHelper.matchFiles(
        FilenameUtils.concat(outputPath, "part-*"));

    HashMap<String, FastQRecord> outReads = new HashMap<String, FastQRecord>();
    for (String oFile : outFiles) {
      FastQFileReader reader = new FastQFileReader(oFile);

      while (reader.hasNext()) {
        FastQRecord record = reader.next();
        record = SpecificData.get().deepCopy(record.getSchema(), record);
        assertFalse(outReads.containsKey(record.getId().toString()));
        outReads.put(record.getId().toString(), record);
      }
    }
    assertEquals(reads, outReads);
  }
}
