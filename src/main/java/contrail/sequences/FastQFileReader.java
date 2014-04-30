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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class FastQFileReader implements Iterator<FastQRecord> {
  private String fastqFile;
  private BufferedReader reader;
  private FastQRecord record;
  private FastQRecord nextRecord;

  private Boolean has_next = null;

  private String line;

  @Deprecated
  public FastQFileReader(String fastqFile) {
    this(fastqFile, new Configuration());
  }

  public FastQFileReader(String fastqFile, Configuration conf) {
    this.fastqFile = fastqFile;
    record = new FastQRecord();
    nextRecord = new FastQRecord();
    try {
      Path path = new Path(fastqFile);
      FSDataInputStream inStream = path.getFileSystem(conf).open(path);
      reader = new BufferedReader(new InputStreamReader(inStream));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(
          "Could not find the fastq file:" + fastqFile);
    }catch (IOException e) {
      throw new RuntimeException(
          "Could not open the fastq file:" + fastqFile, e);
    }
    has_next = null;
  }

  /**
   * Returns the next FastQ record or null if there are no more records.
   *
   * Each call to next uses the same instance of the FastaRecord so data
   * is overwritten on each call.
   */
  @Override
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

  @Override
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

  @Override
  public void finalize() {
    close();
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
