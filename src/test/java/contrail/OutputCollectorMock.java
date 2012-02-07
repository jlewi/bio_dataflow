package contrail;

import org.apache.hadoop.mapred.OutputCollector;



/**
 * A mock class for the hadoop output collector. This is intended
 * for unittesting map functions.
 * 
 * @author jlewi
 *
 */
public class OutputCollectorMock<KEYT, VALUET> implements OutputCollector<KEYT, VALUET>  {

  public KEYT key;
  public VALUET value;

  public void collect(KEYT key, VALUET value) {
    this.key = key;
    this.value = value;    
  }
}
