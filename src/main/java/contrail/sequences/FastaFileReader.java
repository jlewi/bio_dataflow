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

/**
 * Reader for FASTA files.
 */
// TODO(jeremy@lewi.us) How should we handle exceptions? There are various
// options. 1) Propogate them, 2) convert them to RuntimeExceptions and 3)
// catch them and log them.
public class FastaFileReader implements Iterator<FastaRecord> {
  private String fastaFile;
  private BufferedReader reader;
  private FastaRecord record;

  // Variables used for buffering the input.
  private String line;
  private StringBuffer contigSequence = null;

  // The id for the next record or null if there isn't one.
  private String nextId;

  public FastaFileReader(String fastaFile) {
    this.fastaFile = fastaFile;
    record = new FastaRecord();
    try {
      reader = new BufferedReader(
          new FileReader(fastaFile));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          "Could not find the fasta fasta file:" + fastaFile);
    }

    // Advance until the first record.
    try {
      line = reader.readLine();
    } catch (IOException e) {
      throw new RuntimeException(
          "Error reading fasta file:" + fastaFile + " Exception was:" +
          e.getMessage());
    }
    while (line != null) {
      String[] splitLine = line.trim().split("\\s+");
      if (splitLine[0].startsWith(">")) {
        // Store the contigId of the next record.
        nextId = splitLine[0].replaceAll(">", "");
        // Terminate the loop.
        break;
      }

      // Continue reading until we find a contig id.
      try {
        line = reader.readLine();
      } catch (IOException e) {
        throw new RuntimeException(
            "Error reading fasta file:" + fastaFile + " Exception was:" +
            e.getMessage());
      }
    }
  }

  /**
   * Returns the next Fasta record or null if there are no more records.
   *
   * Each call to next uses the same instance of the FastaRecord so data
   * is overwritten on each call.
   */
  public FastaRecord next() {
    if (!hasNext()) {
      return null;
    }

    record.setId(nextId);
    record.setRead(null);
    nextId = null;

    contigSequence = new StringBuffer();

    // The sequence in a fasta file can be spread across multiple
    // lines so we need to keep reading until we see ">" indicating the
    // start of the next record or else the end of the file.
    try {
      line = reader.readLine();
    } catch (IOException e) {
      throw new RuntimeException(
          "Error reading fasta file:" + fastaFile + " Exception was:" +
          e.getMessage());
    }
    while (line != null) {
      if (line.startsWith(">")) {
        // Store the contigId of the next record.
        nextId = line.substring(1);;
        // Terminate the loop.
        break;
      } else {
        contigSequence.append(line);
      }
      try {
        line = reader.readLine();
      } catch (IOException e) {
        throw new RuntimeException(
            "Error reading fasta file:" + fastaFile + " Exception was:" +
            e.getMessage());
      }
    }

    record.setRead(contigSequence);
    return record;
  }

  public boolean hasNext() {
    if (nextId == null) {
      // No more records
      return false;
    }
    return true;
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
