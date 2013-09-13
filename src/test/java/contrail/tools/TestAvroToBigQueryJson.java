/* Licensed under the Apache License, Version 2.0 (the "License");
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
package contrail.tools;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.QuakeReadCorrection;
import contrail.sequences.Read;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestAvroToBigQueryJson {
  @Test
  public void testJob() {
    // Test that the match works when we provide a directory and a glob path.
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();
    Configuration conf = new Configuration();

    String inputPath = FilenameUtils.concat(tempDir, "input");
    String outputPath = FilenameUtils.concat(tempDir, "output");

    Random generator = new Random();
    Alphabet alphabet = DNAAlphabetFactory.create();

    ArrayList<Read> records = new ArrayList<Read>();
    for (int i = 0; i < 10; ++i) {
      Read read = new Read();
      FastQRecord fastq = new FastQRecord();
      read.setFastq(fastq);
      fastq.setId(String.format("read-%d", i));
      fastq.setQvalue("ABCDEF");
      fastq.setRead(AlphabetUtil.randomString(generator, 10, alphabet));
      QuakeReadCorrection correction = new QuakeReadCorrection();
      read.setQuakeReadCorrection(correction);

      correction.setCorrected(false);
      correction.setTrimLength(10);

      records.add(read);
    }
    AvroFileUtil.writeRecords(
        conf, new Path(FilenameUtils.concat(inputPath,  "reads.avro")),
        records);

    AvroToBigQueryJson stage = new AvroToBigQueryJson();
    stage.setParameter("inputpath", inputPath);
    stage.setParameter("outputpath", outputPath);
    assertTrue(stage.execute());
    System.out.println("Outputpath:" + outputPath);
  }
}
