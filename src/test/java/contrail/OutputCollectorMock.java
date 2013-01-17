package contrail;

import java.util.ArrayList;

import org.apache.hadoop.mapred.OutputCollector;



/**
 * A mock class for the hadoop output collector. This is intended
 * for unittesting map functions.
 *
 * @author jlewi
 *
 */
public class OutputCollectorMock<KEYT, VALUET> implements OutputCollector<KEYT, VALUET>  {

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

  public void collect(KEYT key, VALUET value) {
    this.key = key;
    this.value = value;
    outputs.add(new OutputPair(key, value));
  }
}
