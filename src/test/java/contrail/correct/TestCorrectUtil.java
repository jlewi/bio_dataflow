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
package contrail.correct;

import static org.junit.Assert.assertEquals;

import org.apache.avro.specific.SpecificData;
import org.junit.Test;

import contrail.sequences.FastQRecord;
import contrail.sequences.QuakeReadCorrection;
import contrail.sequences.Read;

public class TestCorrectUtil {
  @Test
  public void testFastQToRead() {
    FastQRecord fastq = new FastQRecord();
    Read read = new Read();
    Read expectedRead = new Read();
    CorrectUtil correctUtil = new CorrectUtil();

    fastq.setId("SRR022868.8762453/1");
    fastq.setRead("ACCTG");
    fastq.setQvalue("12345");

    expectedRead.setFastq(
        SpecificData.get().deepCopy(fastq.getSchema(), fastq));
    expectedRead.setQuakeReadCorrection(new QuakeReadCorrection());
    expectedRead.getQuakeReadCorrection().setCorrected(false);
    expectedRead.getQuakeReadCorrection().setTrimLength(0);

    read.setQuakeReadCorrection(new QuakeReadCorrection());
    correctUtil.convertFastQToRead(fastq, read);
    assertEquals(expectedRead, read);

    fastq.setId("SRR022868.8762453/1  correct");
    fastq.setRead("ACCTG");
    fastq.setQvalue("12345");

    expectedRead.setFastq(
        SpecificData.get().deepCopy(fastq.getSchema(), fastq));
    expectedRead.getFastq().setId("SRR022868.8762453/1");
    expectedRead.setQuakeReadCorrection(new QuakeReadCorrection());
    expectedRead.getQuakeReadCorrection().setCorrected(true);
    expectedRead.getQuakeReadCorrection().setTrimLength(0);

    correctUtil.convertFastQToRead(fastq, read);
    assertEquals(expectedRead, read);

    fastq.setId("SRR022868.8762453/1  correct trim=5");
    fastq.setRead("ACCTG");
    fastq.setQvalue("12345");

    expectedRead.setFastq(
        SpecificData.get().deepCopy(fastq.getSchema(), fastq));
    expectedRead.getFastq().setId("SRR022868.8762453/1");
    expectedRead.setQuakeReadCorrection(new QuakeReadCorrection());
    expectedRead.getQuakeReadCorrection().setCorrected(true);
    expectedRead.getQuakeReadCorrection().setTrimLength(5);

    correctUtil.convertFastQToRead(fastq, read);
    assertEquals(expectedRead, read);
  }
}
