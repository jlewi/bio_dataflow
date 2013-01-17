package contrail.stages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableByteArrayInput;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * A mock class for the avro output collector. This is intended
 * for unittesting map functions.
 *
 * @author jlewi
 *
 */
public class AvroCollectorMock<VALUET> extends AvroCollector<VALUET>  {

  public List<VALUET> data = new ArrayList<VALUET>();

  private Schema value_schema;
  private boolean getSchemaFromValue;
  /**
   * Default constructor.
   *
   * This works in most cases and determines the schema from an instance of
   * VALUET.
   */
  public AvroCollectorMock() {
    getSchemaFromValue = true;
  }

  /**
   * Constructor where schema for the value is specified.
   *
   * This is primarily useful when the schema is defined at runtime; e.g
   * when using a union schema.
   */
  public AvroCollectorMock(Schema value_schema) {
    this.value_schema = value_schema;
    getSchemaFromValue = false;
  }

  public void collect(VALUET value) {
    // Make a copy of the object.
    // The object should be an AVRO datum so we could use those methods to
    // copy it.
    Class schemacls = value.getClass();

    if (getSchemaFromValue) {
      try {
        Method get_schema_method;
        get_schema_method = schemacls.getMethod("getSchema");
        value_schema = (Schema) get_schema_method.invoke(value);
      }
      catch (NoSuchMethodException exception) {
        throw new RuntimeException("value doesn't have method getSchema:" + exception.getMessage());
      }
      catch (IllegalAccessException exception) {
        throw new RuntimeException("Problem invoking getSchema on value:" + exception.getMessage());
      }
      catch (InvocationTargetException exception) {
        throw new RuntimeException("Problem invoking getSchema on value:" + exception.getMessage());
      }
    }

    SpecificDatumWriter<VALUET> datum_writer = new SpecificDatumWriter<VALUET>(
        value_schema);
    DataFileWriter<VALUET> file_writer = new DataFileWriter<VALUET>(datum_writer);

    ByteArrayOutputStream out_stream = new ByteArrayOutputStream();

    try {
      file_writer.create(value_schema, out_stream);
      file_writer.append(value);
      file_writer.close();
    }
    catch (IOException exception) {
      throw new RuntimeException("Exception occured while serializaing the data:" + exception.getMessage());
    }

    SeekableByteArrayInput in_stream = new SeekableByteArrayInput(out_stream.toByteArray());
    SpecificDatumReader<VALUET> data_reader = new SpecificDatumReader<VALUET>();

    VALUET copy;

    try {
      DataFileReader<VALUET> file_reader = new DataFileReader<VALUET>(in_stream, data_reader);
      copy = file_reader.next();
      file_reader.close();
    }
    catch (IOException exception) {
      throw new RuntimeException(exception.getMessage());
    }

    data.add(copy);
  }
}
