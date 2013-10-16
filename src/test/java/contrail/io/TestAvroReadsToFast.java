/*
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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.OutputCollectorMock;
import contrail.ReporterMock;
import contrail.correct.CorrectUtil;
import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.QuakeReadCorrection;
import contrail.sequences.Read;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestAvroReadsToFast {
  @Test
  public void testFastQ() {
    JobConf conf = new JobConf();

    Random generator = new Random();
    Alphabet alphabet = DNAAlphabetFactory.create();

    AvroReadsToFast.ToFastMapper mapper = new AvroReadsToFast.ToFastMapper();
    mapper.configure(conf);

    CorrectUtil correctUtil = new CorrectUtil();

    OutputCollectorMock<Text, NullWritable> collector =
        new OutputCollectorMock<Text, NullWritable>(
            Text.class, NullWritable.class);

    // Check it works when input is a Read.
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

      ReporterMock reporter = new ReporterMock();

      // Check it works when input is a Read.
      AvroWrapper<Object> key = new AvroWrapper<Object>();
      key.datum(read);
      try {
        mapper.map(key, NullWritable.get(), collector, reporter);
      } catch (IOException e) {
        fail(e.toString());
      }

      assertEquals(correctUtil.fastqRecordToString(fastq),
                  collector.outputs.get(i).key.toString());
    }

    collector = new OutputCollectorMock<Text, NullWritable>(
        Text.class, NullWritable.class);

    // Check it works when input is a FastQRecord.
    for (int i = 0; i < 10; ++i) {
      FastQRecord fastq = new FastQRecord();
      fastq.setId(String.format("read-%d", i));
      fastq.setQvalue("ABCDEF");
      fastq.setRead(AlphabetUtil.randomString(generator, 10, alphabet));

      ReporterMock reporter = new ReporterMock();

      // Check it works when input is a Read.
      AvroWrapper<Object> key = new AvroWrapper<Object>();
      key.datum(fastq);
      try {
        mapper.map(key, NullWritable.get(), collector, reporter);
      } catch (IOException e) {
        fail(e.toString());
      }

      assertEquals(correctUtil.fastqRecordToString(fastq),
                  collector.outputs.get(i).key.toString());
    }

    try {
      mapper.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }


  @Test
  public void testRun() {
    File tempDir = FileHelper.createLocalTempDir();
    String inputpath = FilenameUtils.concat(tempDir.toString(), "input");
    String outputpath = FilenameUtils.concat(tempDir.toString(), "output");

    Random generator = new Random();
    Alphabet alphabet = DNAAlphabetFactory.create();

    ArrayList<Read> read_records = new ArrayList<Read>();
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

      read_records.add(read);
    }

    ArrayList<FastQRecord> fastq_records = new ArrayList<FastQRecord>();
    // Check it works when input is a FastQRecord.
    for (int i = 0; i < 10; ++i) {
      FastQRecord fastq = new FastQRecord();
      fastq.setId(String.format("read-%d", i));
      fastq.setQvalue("ABCDEF");
      fastq.setRead(AlphabetUtil.randomString(generator, 10, alphabet));
      fastq_records.add(fastq);
    }

    AvroFileUtil.writeRecords(
        new Configuration(),
        new Path(FilenameUtils.concat(inputpath, "fastq.avro")),
        fastq_records);

    AvroFileUtil.writeRecords(
        new Configuration(),
        new Path(FilenameUtils.concat(inputpath, "reads.avro")),
        read_records);

    AvroReadsToFast stage = new AvroReadsToFast();
    stage.setParameter("inputpath", inputpath);
    stage.setParameter("outputpath", outputpath);
    assertTrue(stage.execute());
  }
}
