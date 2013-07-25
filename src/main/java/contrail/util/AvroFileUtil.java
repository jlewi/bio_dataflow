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
package contrail.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AvroFileUtil {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(AvroFileUtil.class);

  /**
   * Read all the records in a file.
   */
  public static <T> ArrayList<T> readRecords(String path, Schema schema) {
    DatumReader<T> datum_reader = new SpecificDatumReader<T>(schema);
    DataFileReader<T> reader = null;
    try {
      reader = new DataFileReader<T>(new File(path), datum_reader);
    } catch(IOException e) {
      sLogger.fatal("Could not read file", e);
    }

    ArrayList<T> output = new ArrayList<T>();
    while(reader.hasNext()){
      T record = reader.next();
      output.add(record);
    }
    return output;
  }

  /***
   * Write a collection of records to an avro file.
   * @param conf
   * @param path
   * @param records
   */
  public static <T extends GenericContainer> void writeRecords(
      Configuration conf, Path path, Collection<T> records) {
    FileSystem fs = null;
    try{
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      sLogger.fatal("Can't get filesystem: " + e.getMessage(), e);
    }

    // Write the data to the file.
    Schema schema = records.iterator().next().getSchema();
    DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(schema);
    DataFileWriter<T> writer = new DataFileWriter<T>(datumWriter);

    try {
      FSDataOutputStream outputStream = fs.create(path);
      writer.create(schema, outputStream);
      for (T record : records) {
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      sLogger.fatal(
          "There was a problem writing the N50 stats to an avro file. " +
          "Exception: " + exception.getMessage(), exception);
    }
  }

  /**
   * Write a collection of records to an avro file.
   *
   * Use this function when the schema can't be inferred from the type of
   * record.
   * @param conf
   * @param path
   * @param records
   */
  public static <T extends GenericContainer> void writeRecords(
      Configuration conf, Path path, Iterable<? extends Object> records,
      Schema schema) {
    FileSystem fs = null;
    try{
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      sLogger.fatal("Can't get filesystem: " + e.getMessage(), e);
    }

    // Write the data to the file.
    DatumWriter<Object> datumWriter = new SpecificDatumWriter<Object>(schema);
    DataFileWriter<Object> writer = new DataFileWriter<Object>(datumWriter);

    try {
      FSDataOutputStream outputStream = fs.create(path);
      writer.create(schema, outputStream);
      for (Object record : records) {
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      sLogger.fatal(
          "There was a problem writing the N50 stats to an avro file. " +
          "Exception: " + exception.getMessage(), exception);
    }
  }

  /**
   * Determine the schema of a file.
   */
  public static Schema readFileSchema(Configuration conf, String path) {
    GenericDatumReader<GenericRecord> reader =
        new GenericDatumReader<GenericRecord>();
    try {
      Path filePath = new Path(path);
      FileSystem fs = filePath.getFileSystem(conf);
      FSDataInputStream inStream = fs.open(filePath);

      DataFileStream<GenericRecord> avroStream =
          new DataFileStream<GenericRecord>(inStream, reader);

      return avroStream.getSchema();
    } catch (IOException exception) {
      throw new RuntimeException(
          "There was a problem reading the nodes the graph to an avro file." +
          " Exception:" + exception.getMessage());
    }
  }
}
