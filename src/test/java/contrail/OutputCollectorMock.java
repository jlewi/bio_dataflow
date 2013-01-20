package contrail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;



/**
 * A mock class for the hadoop output collector. This is intended
 * for unittesting map functions.
 *
 * The key and value must extend Writable because we use those methods to
 * make copies of the objects.
 */
public class OutputCollectorMock<KEYT extends Writable, VALUET extends Writable>
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


  public void collect(KEYT key, VALUET value) {
    this.key = key;
    this.value = value;

    // We need to make copies of the key and value because the object
    // could be reused.
    KEYT kCopy;
    kCopy = copyWritable(key, keyClass);

    VALUET vCopy;
    vCopy = copyWritable(value, valueClass);

    outputs.add(new OutputPair(kCopy, vCopy));
  }
}
