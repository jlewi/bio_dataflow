package contrail;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;


/**
 * A mock class implementing the reporter interface which we can use
 * for unittests.
 * 
 * @author jlewi
 *
 */
public class ReporterMock implements Reporter {

  public Counters.Counter getCounter(Enum<?> name) {
    return null;
  }
  
  public Counters.Counter getCounter(String group, String name) {
    return null;
  }
  
  public InputSplit getInputSplit() {
    return null;
  }
  
  public void incrCounter(Enum<?> key, long amount) {
    // DO nothing    
  }
  
  public void incrCounter(String group, String counter, long amount) {    
    // DO nothing    
  }
  
  public void setStatus(String status) {
    // Do Nothing
  }

  public void progress() {
    //Do nothing
  }
}
