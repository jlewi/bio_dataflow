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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

public class AvroFileUtil {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(AvroFileUtil.class);

  /**
   * Write the records as pretty printed json.
   *
   * Important: The json output is an array of records. Thus, this function is
   * not compatible with readJsonRecords. Use readJsonArray
   */
  public static <T extends GenericContainer> void prettyPrintJsonArray(
        Configuration conf, Path path, Collection<T> records) {
    Schema schema = records.iterator().next().getSchema();

    try {
      FileSystem fs = path.getFileSystem(conf);
      FSDataOutputStream outStream = fs.create(path);
      Schema arraySchema = Schema.createArray(schema);
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(arraySchema);

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());

      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          arraySchema, generator);

      writer.write(records, encoder);
      encoder.flush();

      outStream.flush();
      outStream.close();
    } catch(IOException e){
      sLogger.fatal(
          "There was a problem writing the records to an avro file. " +
          "Exception: " + e.getMessage(), e);
    }
  }

  /**
   * Read a file containing an array of records.
   *
   * @param conf The configuration.
   * @param path The path to the file.
   * @param itemSchema The schema of the items in the array
   */
  public static <T> List<T> readJsonArray(
        Configuration conf, Path path, Schema itemSchema) {
    ArrayList<T> records = new ArrayList<T>();
    Schema arraySchema = Schema.createArray(itemSchema);
    try {
      FSDataInputStream inStream = path.getFileSystem(conf).open(path);
      SpecificDatumReader<List<T>> reader =
          new SpecificDatumReader<List<T>>(arraySchema);
      JsonDecoder decoder = DecoderFactory.get().jsonDecoder(
          arraySchema, inStream);

      List<T> datum = reader.read(null, decoder);
      return datum;
    } catch(IOException e) {
      sLogger.fatal("IOException.", e);
    }
    return null;
  }

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

  /**
   * Read records from a json file produced with PrettyPrint.
   *
   * The file should contain a sequence of white space separated records.
   */
  public static <T> ArrayList<T> readJsonRecords(
      InputStream inStream, Schema schema) {
    ArrayList<T> records = new ArrayList<T>();
    try {
      SpecificDatumReader<T> reader = new SpecificDatumReader<T>(schema);
      JsonDecoder decoder = DecoderFactory.get().jsonDecoder(schema , inStream);

      try {
        while (true) {
          T datum = reader.read(null, decoder);
          records.add(datum);
        }
      } catch(EOFException e) {
        // Reached the end of the file.
      }

    } catch(IOException e) {
      sLogger.fatal("IOException.", e);
    }
    return records;
  }

  /**
   * Read records from a json file produced with PrettyPrint.
   */
  public static <T> ArrayList<T> readJsonRecords(
      Configuration conf, Path path, Schema schema) {
    ArrayList<T> records = null;
    try {
      FSDataInputStream inStream = path.getFileSystem(conf).open(path);

      records = readJsonRecords(inStream, schema);
      inStream.close();
    } catch(IOException e) {
      sLogger.fatal("IOException.", e);
    }
    return records;
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
          "There was a problem writing the the records to an avro file. " +
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
          "There was a problem writing the records to an avro file. " +
          "Exception: " + exception.getMessage(), exception);
    }
  }

  /**
   * Write a collection of records to an avro file.
   *
   * Use this function when the schema can't be inferred from the type of
   * record.
   *
   * The stream is closed by the function.
   *
   * @param conf
   * @param path
   * @param records
   */
  public static <T extends GenericContainer> void writeRecords(
      OutputStream outputStream, Iterable<? extends Object> records,
      Schema schema) {
    // Write the data to the file.
    DatumWriter<Object> datumWriter = new SpecificDatumWriter<Object>(schema);
    DataFileWriter<Object> writer = new DataFileWriter<Object>(datumWriter);

    try {
      writer.create(schema, outputStream);
      for (Object record : records) {
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      sLogger.fatal(
          "There was a problem writing the records to an avro file. " +
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
