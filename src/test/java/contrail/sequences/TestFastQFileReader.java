/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.sequences;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import org.junit.Test;

public class TestFastQFileReader {
  /**
   * Write the records to a fasta file.
   */
  private void writeFastQFile(File path, Collection<FastQRecord> records) {
    try {
      FileWriter stream = new FileWriter(path, true);
      BufferedWriter out = new BufferedWriter(stream);

      for (FastQRecord record : records) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("@");
        buffer.append(record.getId());
        buffer.append("\n");
        buffer.append(record.getRead());
        buffer.append("\n+\n");
        buffer.append(record.getQvalue());
        buffer.append("\n");

        out.write(buffer.toString());
      }
      out.close();
      stream.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void test() {
    // Create some records and write them to a file.
    Random generator = new Random();
    ArrayList<FastQRecord> records = new ArrayList<FastQRecord>();
    for (int i = 0; i < 10; ++i) {
      FastQRecord record = new FastQRecord();
      record.setId("read_" + i);
      record.setRead(AlphabetUtil.randomString(
          generator, 100, DNAAlphabetFactory.create()));
      record.setQvalue(AlphabetUtil.randomString(
          generator, 100, DNAAlphabetFactory.create()));
      records.add(record);
    }

    File tempFile = null;
    try {
       tempFile = File.createTempFile("temp", ".fastq");
    } catch (IOException e) {
      fail("Couldn't create temp file:" + e.getMessage());
    }
    writeFastQFile(tempFile, records);

    // Read the records and make sure they match the input.
    ArrayList<FastQRecord> actualRecords = new ArrayList<FastQRecord>();
    FastQFileReader reader = new FastQFileReader(tempFile.getPath());
    while (reader.hasNext()) {
      FastQRecord record = reader.next();
      FastQRecord copy = new FastQRecord();
      copy.setId(record.getId().toString());
      copy.setRead(record.getRead().toString());
      copy.setQvalue(record.getQvalue().toString());
      actualRecords.add(copy);
    }
    assertEquals(records, actualRecords);

    // Clean up the temporary file.
    if (tempFile.exists()) {
      tempFile.delete();
    }
  }
}

