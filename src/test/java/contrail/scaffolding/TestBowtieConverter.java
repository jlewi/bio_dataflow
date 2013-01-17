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
import java.util.ArrayList;

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
  public class TestData {
    final public String input;
    final public BowtieMapping expected;

    TestData(String input, BowtieMapping expected) {
      this.input = input;
      this.expected = expected;
    }
  }

  @Test
  public void TestMapper() {
    ArrayList<TestData> pairs = new ArrayList<TestData>();

    {
      // Simple test case no mismatches.
      String sequence = "ACCTTATAAAAACATGAAAA";
      String inputLine = String.format(
          "read_2_0_75 + contig_2_0  75  %s  " +
          "IIIIIIIIIIIIIIIIIIII  0", sequence);

      BowtieMapping expected = new BowtieMapping();
      expected.setContigId("contig_2_0");
      expected.setReadId("read_2_0_75");
      expected.setContigStart(75);
      expected.setContigEnd(75 + sequence.length() - 1);
      expected.setReadClearStart(0);
      expected.setReadClearEnd(sequence.length() - 1);
      expected.setNumMismatches(0);
      expected.setRead(sequence);

      pairs.add(new TestData(inputLine, expected));
    }

    {
      // 1 mismatch.
      String sequence = "ACCTTATAAAAACATGAAAA";
      String inputLine = String.format(
          "read_2_0_75 + contig_2_0  75  %s  " +
          "IIIIIIIIIIIIIIIIIIII  0  21:A>C", sequence);

      BowtieMapping expected = new BowtieMapping();
      expected.setContigId("contig_2_0");
      expected.setReadId("read_2_0_75");
      expected.setContigStart(75);
      expected.setContigEnd(75 + sequence.length() - 1);
      expected.setReadClearStart(0);
      expected.setReadClearEnd(sequence.length() - 1);
      expected.setNumMismatches(1);
      expected.setRead(sequence);

      pairs.add(new TestData(inputLine, expected));
    }

    {
      // 2 mismatch.
      String sequence = "ACCTTATAAAAACATGAAAA";
      String inputLine = String.format(
          "read_2_0_75 + contig_2_0  75  %s  " +
          "IIIIIIIIIIIIIIIIIIII  0  21:A>C,23:A>C", sequence);

      BowtieMapping expected = new BowtieMapping();
      expected.setContigId("contig_2_0");
      expected.setReadId("read_2_0_75");
      expected.setContigStart(75);
      expected.setContigEnd(75 + sequence.length() - 1);
      expected.setReadClearStart(0);
      expected.setReadClearEnd(sequence.length() - 1);
      expected.setNumMismatches(2);
      expected.setRead(sequence);

      pairs.add(new TestData(inputLine, expected));
    }

    JobConf job = new JobConf(BowtieConverter.ConvertMapper.class);
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    for (TestData pair : pairs) {
      OutputCollectorMock<AvroWrapper<BowtieMapping>, NullWritable>
          collector_mock =
            new OutputCollectorMock<AvroWrapper<BowtieMapping>, NullWritable>();

      BowtieConverter.ConvertMapper mapper =
          new BowtieConverter.ConvertMapper();
      mapper.configure(job);

      Text input = new Text(pair.input);
      try {
        mapper.map(new LongWritable(1), input, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
      // Check the values are equal.
      assertEquals(pair.expected, collector_mock.key.datum());
    }
  }
}
