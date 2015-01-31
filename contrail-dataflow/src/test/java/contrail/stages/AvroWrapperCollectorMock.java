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
package contrail.stages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * A mock collector for working with non-avro mappers which output
 * avro values; i.e the mapper outputs AvroWrapper<T>, NullWritable
 *
 * We can't use AvroCollectorMock or OutputCollectorMock because
 * of the way we clone the datum being collected.
 */
public class AvroWrapperCollectorMock<
  VALUET extends org.apache.avro.specific.SpecificRecordBase> implements
  OutputCollector<AvroWrapper<VALUET>, NullWritable> {

  public List<AvroWrapper<VALUET>> data = new ArrayList<AvroWrapper<VALUET>>();

  /**
   * Default constructor.
   */
  public AvroWrapperCollectorMock() {
  }

  public void collect(AvroWrapper<VALUET> value, NullWritable nullObject) {
    SpecificDatumWriter<VALUET> datum_writer = new SpecificDatumWriter<VALUET>(
        value.datum().getSchema());
    DataFileWriter<VALUET> file_writer = new DataFileWriter<VALUET>(datum_writer);

    ByteArrayOutputStream out_stream = new ByteArrayOutputStream();

    try {
      file_writer.create(value.datum().getSchema(), out_stream);
      file_writer.append(value.datum());
      file_writer.close();
    }
    catch (IOException exception) {
      throw new RuntimeException("Exception occured while serializaing the data:" + exception.getMessage());
    }

    SeekableByteArrayInput in_stream = new SeekableByteArrayInput(out_stream.toByteArray());
    SpecificDatumReader<VALUET> data_reader = new SpecificDatumReader<VALUET>();

    AvroWrapper<VALUET> copy = new AvroWrapper<VALUET>();

    try {
      DataFileReader<VALUET> file_reader = new DataFileReader<VALUET>(in_stream, data_reader);
      VALUET datum = file_reader.next();
      file_reader.close();
      copy.datum(datum);
    }
    catch (IOException exception) {
      throw new RuntimeException(exception.getMessage());
    }

    data.add(copy);
  }
}
