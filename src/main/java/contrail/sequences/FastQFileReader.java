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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class FastQFileReader implements Iterator<FastQRecord> {
  private String fastqFile;
  private BufferedReader reader;
  private FastQRecord record;
  private FastQRecord nextRecord;

  private Boolean has_next = null;

  private String line;

  public FastQFileReader(String fastqFile) {
    this.fastqFile = fastqFile;
    record = new FastQRecord();
    nextRecord = new FastQRecord();
    try {
      reader = new BufferedReader(
          new FileReader(fastqFile));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          "Could not find the fasta fasta file:" + fastqFile);
    }
    has_next = null;
  }

  /**
   * Returns the next FastQ record or null if there are no more records.
   *
   * Each call to next uses the same instance of the FastaRecord so data
   * is overwritten on each call.
   */
  public FastQRecord next() {
    if (!hasNext()) {
      return null;
    }

    // Cycle the pointers. We do this because we want to reuse existing
    // storage rather than creating new instances.
    FastQRecord temp = record;
    record = nextRecord;
    nextRecord = temp;

    has_next = null;
    return record;
  }

  public boolean hasNext() {
    if (has_next == null) {
      // Try to read the next record.
      try {
        line = reader.readLine();
        while ((line != null) && (!line.startsWith("@"))) {
          line = reader.readLine();
        }
        if (line == null) {
          has_next = false;
          return has_next;
        }

        // TODO(jeremy@lewi.us): We should add better handling for bad
        // records or corrupted data.
        nextRecord.setId(line.subSequence(1, line.length()));
        line = reader.readLine();
        nextRecord.setRead(line);
        line = reader.readLine();
        line = reader.readLine();
        nextRecord.setQvalue(line);
        has_next = true;
      } catch (IOException e) {
        throw new RuntimeException(
            "Could not read the fastq file:" + this.fastqFile);
      }
    }
    return has_next;
  }

  /**
   * Close the file.
   */
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(
            "Exception occured while trying to close the fasta file:" +
                e.getMessage());
      }
    }
  }

  public void finalize() {
    close();
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}
