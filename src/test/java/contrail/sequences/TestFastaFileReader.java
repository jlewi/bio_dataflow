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

public class TestFastaFileReader {

  /**
   * Write the records to a fasta file.
   */
  private void writeFastaFile(File path, Collection<FastaRecord> records) {
    try {
      FileWriter stream = new FileWriter(path, true);
      BufferedWriter out = new BufferedWriter(stream);

      for (FastaRecord record : records) {
        StringBuffer buffer = new StringBuffer();
        buffer.append(">");
        buffer.append(record.getId());
        buffer.append("\n");
        // Split the sequence across two lines to make sure we can
        // handle sequence not being on one line.
        int length = record.getRead().length();
        int midPoint = length / 2;
        buffer.append(record.getRead().subSequence(0, midPoint));
        buffer.append("\n");
        buffer.append(record.getRead().subSequence(midPoint, length));
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
    ArrayList<FastaRecord> records = new ArrayList<FastaRecord>();
    for (int i = 0; i < 10; ++i) {
      FastaRecord record = new FastaRecord();
      record.setId("read_" + i);
      record.setRead(AlphabetUtil.randomString(
          generator, 100, DNAAlphabetFactory.create()));
      records.add(record);
    }

    File tempFile = null;
    try {
       tempFile = File.createTempFile("temp", ".Fasta");
    } catch (IOException e) {
      fail("Couldn't create temp file:" + e.getMessage());
    }
    writeFastaFile(tempFile, records);

    // Read the records and make sure they match the input.
    ArrayList<FastaRecord> actualRecords = new ArrayList<FastaRecord>();
    FastaFileReader reader = new FastaFileReader(tempFile.getPath());
    while (reader.hasNext()) {
      FastaRecord record = reader.next();
      FastaRecord copy = new FastaRecord();
      copy.setId(record.getId().toString());
      copy.setRead(record.getRead().toString());
      actualRecords.add(copy);
    }
    assertEquals(records, actualRecords);

    // Clean up the temporary file.
    if (tempFile.exists()) {
      tempFile.delete();
    }
  }
}
