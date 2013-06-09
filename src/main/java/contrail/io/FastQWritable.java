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
package contrail.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Hadoop Writable object for FastQ records.
 *
 */
public class FastQWritable implements Writable {
  // Some data
  private String id;
  private String dna;
  private String qValue;

  public void write(DataOutput out) throws IOException {
    out.writeBytes(toString());
  }

  public void readFields(DataInput in) throws IOException {
    id = in.readLine();
    if (id == null) {
      throw new EOFException();
    }
    // First character should be the @ symbol.
    if (id.charAt(0) == '@') {
      id = id.substring(1);
    }
    dna = in.readLine();
    // Skip the next line.
    String line = in.readLine();
    if (line == null) {
      throw new EOFException();
    }
    qValue = in.readLine();
    if (qValue == null) {
      throw new EOFException();
    }
  }

  public static FastQWritable read(DataInput in) throws IOException {
    FastQWritable w = new FastQWritable();
    w.readFields(in);
    return w;
  }

  public String getId() {
    return id;
  }

  public String getDNA() {
    return dna;
  }

  public String getQValue() {
    return qValue;
  }

  public void setId(String id) {
    this.id = id;
  }

  public void setDNA(String dna) {
    this.dna = dna;
  }

  public void setQValue(String qValue) {
    this.qValue = qValue;
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("@" + id + "\n");
    builder.append(dna + "\n");
    builder.append("+\n");
    builder.append(qValue);
    return builder.toString();
  }
}
