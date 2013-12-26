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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.Test;

public class TestFastUtil {

  @Test
  public void testWriteFastQRecord() {
    FastQRecord record = new FastQRecord();
    record.setId("Some read");
    record.setRead("ACTTTA");
    record.setQvalue("!@!!!!");
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(outStream);
    FastUtil.writeFastQRecord(stream, record);
    stream.close();
    String actualValue = outStream.toString();
    String expectedValue =
        String.format(
            "@%s\n%s\n+\n%s\n", record.getId(), record.getRead(),
            record.getQvalue());

    assertEquals(expectedValue, actualValue);
  }

  @Test
  public void testWriteFastQRecordToStream() throws IOException {
    FastQRecord record = new FastQRecord();
    record.setId("Some read");
    record.setRead("ACTTTA");
    record.setQvalue("!@!!!!");
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    FastUtil.writeFastQRecord(outStream, record);
    outStream.close();
    String actualValue = outStream.toString();
    String expectedValue =
        String.format(
            "@%s\n%s\n+\n%s\n", record.getId(), record.getRead(),
            record.getQvalue());

    assertEquals(expectedValue, actualValue);
  }
}
