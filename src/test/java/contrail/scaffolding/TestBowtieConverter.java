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
package contrail.scaffolding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.OutputCollectorMock;
import contrail.ReporterMock;

public class TestBowtieConverter {
  @Test
  public void TestMapper() {
    String sequence = "ACCTTATAAAAACATGAAAA";
    String inputLine = String.format(
        "read_2_0_75 + contig_2_0  75  %s  " +
        "IIIIIIIIIIIIIIIIIIII  0", sequence);
    OutputCollectorMock<AvroWrapper<BowtieMapping>, NullWritable>
        collector_mock =
          new OutputCollectorMock<AvroWrapper<BowtieMapping>, NullWritable>();

    JobConf job = new JobConf(BowtieConverter.ConvertMapper.class);
    job.setInt("sub_length", 25);
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    BowtieConverter.ConvertMapper mapper = new BowtieConverter.ConvertMapper();
    mapper.configure(job);

    Text input = new Text(inputLine);
    try {
      mapper.map(new LongWritable(1), input, collector_mock, reporter);
    }
    catch (IOException exception){
      fail("IOException occured in map: " + exception.getMessage());
    }

    BowtieMapping expected = new BowtieMapping();
    expected.setContigId("contig_2_0");
    expected.setReadId("read_2_0_75");
    expected.setContigStart(75);
    expected.setContigEnd(75 + sequence.length() - 1);
    expected.setRead(sequence);

    // Check the values are equal.
    assertEquals(expected, collector_mock.key.datum());
  }
}
