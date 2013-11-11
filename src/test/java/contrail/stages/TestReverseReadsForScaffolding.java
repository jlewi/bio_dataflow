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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.apache.avro.specific.SpecificData;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.sequences.Sequence;
import contrail.util.FileHelper;

public class TestReverseReadsForScaffolding {
  @Test
  public void testRun() {
    Random generator = new Random();
    int numFiles = 4;
    int numReads = 20;
    int readLength = 2000;

    File tempDir = FileHelper.createLocalTempDir();
    String outputPath = FilenameUtils.concat(tempDir.getPath(), "output");
    String inputPath = FilenameUtils.concat(tempDir.getPath(), "input");

    // Create the directory for the input.
    new File(inputPath).mkdir();

    // Keep track of the expected reads for each file.
    ArrayList<HashMap<String, FastQRecord>> expected =
        new ArrayList<HashMap<String, FastQRecord>>();


    for (int fileIndex = 0; fileIndex < numFiles; ++fileIndex) {
      HashMap<String, FastQRecord> reads = new HashMap<String, FastQRecord>();

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

        // Reverse the read.
        FastQRecord reversed = new FastQRecord();
        Sequence sequence = new Sequence(
            read.getRead().toString(), DNAAlphabetFactory.create());
        reversed.setRead(DNAUtil.reverseComplement(sequence).toString());
        reversed.setId(read.getId().toString());
        reversed.setQvalue(StringUtils.reverse(read.getQvalue().toString()));
        reads.put(read.getId().toString(), reversed);
      }

      expected.add(reads);
      outStream.close();
    }

    // Run it.
    ReverseReadsForScaffolding stage = new ReverseReadsForScaffolding();
    HashMap<String, Object> stageOptions = new HashMap<String, Object>();
    stageOptions.put("inputpath", FilenameUtils.concat(inputPath, "*.fq"));
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);

    if (!stage.execute()) {
      fail("ReverseReads failed.");
    }

    ArrayList<String> outFiles = FileHelper.matchFiles(
        FilenameUtils.concat(outputPath, "reads_*.fq"));

    Collections.sort(outFiles);
    assertEquals(numFiles, outFiles.size());

    for (int i = 0; i < numFiles; ++i) {
      String oFile = outFiles.get(i);
      HashMap<String, FastQRecord> outReads =
          new HashMap<String, FastQRecord>();
      FastQFileReader reader = new FastQFileReader(oFile);

      while (reader.hasNext()) {
        FastQRecord record = reader.next();
        record = SpecificData.get().deepCopy(record.getSchema(), record);
        assertFalse(outReads.containsKey(record.getId().toString()));
        outReads.put(record.getId().toString(), record);
      }
      assertEquals(expected.get(i), outReads);
    }
  }
}
