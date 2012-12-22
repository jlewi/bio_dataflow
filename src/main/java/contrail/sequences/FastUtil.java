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

import java.io.PrintStream;

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
}
