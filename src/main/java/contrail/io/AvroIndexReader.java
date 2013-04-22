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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.hadoop.file.SortedKeyValueFile;
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * A generic class for working with Avro indexed files.
 *
 * The class supports a very limited subset of the Map interface. This
 * allows the class to be used any place where a Map could be used and vice
 * versa.
 *
 * @param <K>: The type for the key of the index.
 * @param <V>: The value for the records.
 */
public class AvroIndexReader<K, V> implements IndexedRecords<K, V> {
//TODO(jeremy@lewi.us): Move this into contrail.graph
 // TODO(jeremy@lewi.us): IndexedGraph should be an abstract base class
 // one subclass could be backed by a sorted avro file, another could be
 // backed by a hashmap.
 // TODO(jeremy@lewi.us): Should we implement a cache to cache recently looked
 // up nodes? If we do we should probably clone the node before returning it.
 private static final Logger sLogger = Logger.getLogger(AvroIndexReader.class);
 private SortedKeyValueFile.Reader<K, V> reader;

 private Schema keySchema;
 private Schema valueSchema;

 /**
  * Create the indexed graph based on the inputpath.
  * @param inputPath: Path to the graph.
  * @param kSchema: The schema for the key. This must match V or else you
  *   will have problems
  * @param vSchema: The schema for the value. This must match V or else you
  *   will have problems.
  */
 public AvroIndexReader(
     String inputPath, Configuration conf, Schema kSchema, Schema vSchema) {
   keySchema = kSchema;
   valueSchema = vSchema;
   createReader(inputPath, conf);
 }

 private void createReader(String inputPath, Configuration conf) {
   SortedKeyValueFile.Reader.Options readerOptions =
       new SortedKeyValueFile.Reader.Options();

   readerOptions.withPath(new Path(inputPath));

   readerOptions.withConfiguration(conf);
   readerOptions.withKeySchema(keySchema);
   readerOptions.withValueSchema(valueSchema);

   reader = null;
   try {
     reader = new SortedKeyValueFile.Reader<K, V> (
         readerOptions);
   } catch (IOException e) {
     sLogger.fatal("Couldn't open indexed avro file.", e);
   }
 }

 /**
  * Read the record for the associated key from the file;
  * @param reader
  * @param nodeId
  * @return
  */
 public V get(K key) {
   GenericRecord record = null;
   V recordData = null;
   try{
     // The actual type returned by get is a generic record even
     // though the return type is GraphNodeData. I have no idea
     // how SortedKeyValueFileReader actually compiles.
     record =  (GenericRecord) reader.get(key);
   } catch (IOException e) {
     sLogger.fatal("There was a problem reading from the file.", e);
     System.exit(-1);
   }
   if (record == null) {
     sLogger.fatal(
         "Could not find key:" + key.toString(),
         new RuntimeException("Couldn't find node"));
     System.exit(-1);
   }

   // Convert the Generic record to a specific record.
   try {
     // TODO(jeremy@lewi.us): We could probably make this code
     // more efficient by reusing objects.
     GenericDatumWriter<GenericRecord> datumWriter =
         new GenericDatumWriter<GenericRecord>(record.getSchema());

     ByteArrayOutputStream outStream = new ByteArrayOutputStream();
     BinaryEncoder encoder =
         EncoderFactory.get().binaryEncoder(outStream, null);

     datumWriter.write(record, encoder);
     // We need to flush the encoder to write the data to the byte
     // buffer.
     encoder.flush();
     outStream.flush();

     // Now read it back in as a specific datum reader.
     ByteArrayInputStream inStream = new ByteArrayInputStream(
         outStream.toByteArray());

     BinaryDecoder decoder =
         DecoderFactory.get().binaryDecoder(inStream, null);
     SpecificDatumReader<V> specificReader = new
         SpecificDatumReader<V>(valueSchema);

     recordData = specificReader.read(recordData, decoder);
   } catch (IOException e) {
     sLogger.fatal(
         "There was a problem converting the GenericRecord to " +
         "GraphNodeData", e);
     System.exit(-1);
   }
   return recordData;
 }

 /**
 * @return An iterator over the key value pairs.
  *
  */
 public Iterator<AvroKeyValue<K, V>> iterator() {
   return reader.iterator();
 }
}
