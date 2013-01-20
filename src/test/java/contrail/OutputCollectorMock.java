package contrail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;



/**
 * A mock class for the hadoop output collector. This is intended
 * for unittesting map functions.
 *
 * @author jlewi
 *
 */
public class OutputCollectorMock<KEYT, VALUET>
    implements OutputCollector<KEYT, VALUET>  {

  private Class<KEYT> keyClass;
  private Class<VALUET> valueClass;

  /*
   * Instantiate the collector.
   *
   * The collector takes class descriptors so that we can create instances.
   */
  public OutputCollectorMock(Class<KEYT> keyClass, Class<VALUET> valueClass) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  public class OutputPair {
    final public KEYT key;
    final public VALUET value;
    public OutputPair(KEYT k, VALUET v) {
      this.key = k;
      this.value =v;
    }
  }

  public ArrayList<OutputPair> outputs = new ArrayList<OutputPair>();

  // These fields are now deprecated because the original mock only
  // supported outputting a single key value pair.
  @Deprecated
  public KEYT key;
  @Deprecated
  public VALUET value;

  private <T> T copyWritable(T datum, Class<T> tClass) {
    // We can't clone a nullwritable because it is a singleton.
    if (datum instanceof NullWritable) {
      return (T) NullWritable.get();
    }

    T newDatum = null;
    try {
      newDatum = tClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream outStream = new DataOutputStream(byteStream);

    try {
      ((Writable) datum).write(outStream);
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem copying the data:" + e.getMessage());
    }

    ByteArrayInputStream inBytes = new ByteArrayInputStream(
        byteStream.toByteArray());
    DataInputStream inStream = new DataInputStream(inBytes);

    try {
      ((Writable)newDatum).readFields(inStream);
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem copying the data:" + e.getMessage());
    }

    return newDatum;
  }


  private <T extends org.apache.avro.specific.SpecificRecordBase> T
    copyAvroDatum(T datum) {
    // Make a copy of the object.
    // The object should be an AVRO datum so we could use those methods to
    // copy it.
    // Class schemacls = datum.getClass();

    Schema schema = datum.getSchema();
//    try {
//      Method get_schema_method;
//      get_schema_method = schemacls.getMethod("getSchema");
//      schema = (Schema) get_schema_method.invoke(value);
//    }
//    catch (NoSuchMethodException exception) {
//      throw new RuntimeException("value doesn't have method getSchema:" + exception.getMessage());
//    }
//    catch (IllegalAccessException exception) {
//      throw new RuntimeException("Problem invoking getSchema on value:" + exception.getMessage());
//    }
//    catch (InvocationTargetException exception) {
//      throw new RuntimeException("Problem invoking getSchema on value:" + exception.getMessage());
//    }


    SpecificDatumWriter<T> datumWriter = new SpecificDatumWriter<T>(schema);
    DataFileWriter<T> file_writer = new DataFileWriter<T>(datumWriter);

    ByteArrayOutputStream out_stream = new ByteArrayOutputStream();

    try {
      file_writer.create(schema, out_stream);
      file_writer.append(datum);
      file_writer.close();
    }
    catch (IOException exception) {
      throw new RuntimeException("Exception occured while serializaing the data:" + exception.getMessage());
    }

    // Read it back in.
    SeekableByteArrayInput in_stream = new SeekableByteArrayInput(
        out_stream.toByteArray());
    SpecificDatumReader<T> data_reader = new SpecificDatumReader<T>();

    T copy;

    try {
      DataFileReader<T> file_reader = new DataFileReader<T>(
          in_stream, data_reader);
      copy = file_reader.next();
      file_reader.close();
    }
    catch (IOException exception) {
      throw new RuntimeException(exception.getMessage());
    }

    return copy;
  }

  private <T extends org.apache.avro.specific.SpecificRecordBase>
    AvroWrapper<T> copyAvroWrapper(
      AvroWrapper<T> wrapped, Class<AvroWrapper<T>> wrapperClass) {
    AvroWrapper<T> copy;
    try {
      copy = wrapperClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    wrapped.datum().getClass().cast(copyAvroDatum(wrapped.datum()));

    T datumCopy = copyAvroDatum(wrapped.datum());
    copy.datum(datumCopy);
    return copy;
  }

  public void collect(KEYT key, VALUET value) {
    this.key = key;
    this.value = value;

    // We need to make copies of the key and value because the object
    // could be reused.
    KEYT kCopy;
    if (key instanceof Writable) {
      kCopy = copyWritable(key, keyClass);
    } else if (key instanceof AvroWrapper<?>) {
      kCopy = copyAvroWrapper( AvroWrapper<?> key, keyClass);

      //wrapperSetDatum(kCopy, copyAvroDatum())
      // Use introspection to get the method for setting the datum.
//      try {
//        Method setDatumMethod;
//        setDatumMethod = kCopy.getClass().getMethod(
//            "datum", );
//        setDatumMethod.invoke(value);
//      }
//      catch (NoSuchMethodException exception) {
//        throw new RuntimeException("value doesn't have method getSchema:" + exception.getMessage());
//      }
//      catch (IllegalAccessException exception) {
//        throw new RuntimeException("Problem invoking getSchema on value:" + exception.getMessage());
//      }
//      catch (InvocationTargetException exception) {
//        throw new RuntimeException("Problem invoking getSchema on value:" + exception.getMessage());
//      }
    }

    VALUET vCopy;
    if (value instanceof Writable) {
      vCopy = copyWritable(value, valueClass);
    } else if (value instanceof AvroWrapper<?>) {
      vCopy = copyAvro(value, valueClass);
    }

    outputs.add(new OutputPair(kCopy, vCopy));
  }
}
