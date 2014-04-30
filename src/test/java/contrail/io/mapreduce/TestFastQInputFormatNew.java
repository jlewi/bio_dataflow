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
package contrail.io.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;

import org.apache.avro.specific.SpecificData;
import org.apache.commons.io.FilenameUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import contrail.io.FastQWritable;
import contrail.sequences.FastQDoFns;
import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestFastQInputFormatNew {
  // The name of the file resource to use. This resource should be the fastq
  // file which contains some fastq records.
  private static String fastqResourcePath =
      "contrail/io/mapreduce/some_reads.fastq";

  @Test
  public void testInputFormat() {
    // We test the new input format by trying to read the file using a crunch
    // pipeline.
    File tempDir = FileHelper.createLocalTempDir();
    tempDir.deleteOnExit();

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(
        TestFastQInputFormatNew.class, new Configuration());

    String inputFile = this.getClass().getClassLoader().getResource(
        fastqResourcePath).toString();

    TableSource<LongWritable, FastQWritable> source = From.formattedFile(
        inputFile, FastQInputFormatNew.class, LongWritable.class,
        FastQWritable.class);

    // See https://issues.apache.org/jira/browse/CRUNCH-369
    // This issue doesn't appear to be fixed in 0.9.0 but should hopefully
    // be fixed in version 0.10.0.
    source.inputConf(
        RuntimeParameters.DISABLE_COMBINE_FILE, Boolean.TRUE.toString());

    PTable<LongWritable, FastQWritable> reads = pipeline.read(source);

    PCollection<FastQRecord> output = reads.values().parallelDo(
        new FastQDoFns.WritableToAvroDo(),
        Avros.specifics(FastQRecord.class));

    String outputPath = FilenameUtils.concat(
        tempDir.getAbsolutePath(), "output");
    Target target = To.avroFile(outputPath);
    output.write(target);

    pipeline.run();

    ArrayList<FastQRecord> written = AvroFileUtil.readRecords(
        FilenameUtils.concat(outputPath, "part-m-00000.avro"),
        new FastQRecord().getSchema());

    FastQFileReader reader = new FastQFileReader(
        inputFile, new Configuration());

    ArrayList<FastQRecord> expected = new ArrayList<FastQRecord>();
    while (reader.hasNext()) {
      FastQRecord record = reader.next();
      record = SpecificData.get().deepCopy(record.getSchema(), record);
      expected.add(record);
    }
    assertEquals(expected.size(), written.size()) ;
    for (int i = 0; i < expected.size(); ++i) {
      assertEquals(expected.get(i), written.get(i));
    }
  }
}