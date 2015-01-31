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
package contrail.correct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.util.FileHelper;

public class TestFastQToAvro extends FastQToAvro {
  @Test
  public void testRun() {
    Random generator = new Random();
    int numFiles = 1;
    int numReads = 100;
    int readLength = 2000;

    File tempDir = FileHelper.createLocalTempDir();
    String outputPath = FilenameUtils.concat(tempDir.getPath(), "output");
    String inputPath = FilenameUtils.concat(tempDir.getPath(), "inputpath");
    new File(inputPath).mkdir();

    HashMap<String, FastQRecord> reads = new HashMap<String, FastQRecord>();

    for (int fileIndex = 0; fileIndex < numFiles; ++fileIndex) {
      File fastqFile = new File(
          inputPath, String.format("reads_%d.fq", fileIndex));
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
        reads.put(read.getId().toString(), read);
      }

      outStream.close();
    }

    // Run it.
    FastQToAvro stage = new FastQToAvro();

    HashMap<String, Object> stageOptions = new HashMap<String, Object>();
    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    stageOptions.put("splitSize", 50000L);
    stage.setParameters(stageOptions);

    if (!stage.execute()) {
      fail("FastQToAvro failed.");
    }

    long numInputs = stage.getNumMapInputRecords();
    long numOutputs = stage.getCounter(
        "org.apache.hadoop.mapred.Task$Counter",
        "MAP_OUTPUT_RECORDS");

    assertEquals(numReads * numFiles, numInputs);
    assertEquals(numReads * numFiles, numOutputs);

    ArrayList<String> outFiles = FileHelper.matchFiles(
        FilenameUtils.concat(outputPath, "part-*.avro"));

    HashMap<String, FastQRecord> outReads = new HashMap<String, FastQRecord>();
    for (String oFile : outFiles) {
      DatumReader<FastQRecord> datumReader =
          new SpecificDatumReader<FastQRecord>((new FastQRecord()).getSchema());
      DataFileReader<FastQRecord> reader = null;
      try {
          reader = new DataFileReader<FastQRecord>(
              new File(oFile), datumReader);
      } catch (IOException io) {
        fail("Could not open the output:" + io.getMessage());
      }

      while (reader.hasNext()) {
        FastQRecord record = reader.next();
        assertFalse(outReads.containsKey(record.getId().toString()));
        outReads.put(record.getId().toString(), record);
      }
    }
    assertEquals(reads, outReads);
  }
}
