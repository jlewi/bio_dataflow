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
package contrail.sequences;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.commons.lang.StringUtils;

/**
 * Utilities for working with fastq and fasta data.
 *
 */
public class FastUtil {
  /**
   * Write a fastq record to the stream.
   *
   * @param record
   */
  public static void writeFastQRecord(PrintStream stream, FastQRecord record) {
    stream.append("@");
    stream.append(record.getId());
    stream.append("\n");
    stream.append(record.getRead());
    stream.append("\n+\n");
    stream.append(record.getQvalue());
    stream.append("\n");
  }

  /**
   * Write a fastq record to the stream.
   *
   * @param record
   */
  public static void writeFastQRecord(OutputStream stream, FastQRecord record)
      throws IOException {
    StringBuffer buffer = new StringBuffer();
    buffer.append("@");
    buffer.append(record.getId());
    buffer.append("\n");
    buffer.append(record.getRead());
    buffer.append("\n+\n");
    buffer.append(record.getQvalue());
    buffer.append("\n");
    stream.write(buffer.toString().getBytes());
  }

  /**
   * Return the record. There is no newline at the end.
   * @param record
   * @return
   */
  public static String fastQRecordToString(FastQRecord record) {
    return StringUtils.join(
        new CharSequence[]{"@" + record.getId(), record.getRead(), "+",
                           record.getQvalue()},
        "\n");

  }

  /**
   * Write a fastA record to the stream.
   *
   * @param record
   */
  public static void writeFastARecord(PrintStream stream, FastaRecord record) {
    stream.append(">");
    stream.append(record.getId());
    stream.append("\n");
    stream.append(record.getRead());
    stream.append("\n");
  }
}
