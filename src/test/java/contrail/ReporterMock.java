package contrail;

import java.util.HashMap;

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
  private final HashMap<String, HashMap<String, Long>> counters;

  public ReporterMock() {
    counters = new HashMap<String, HashMap<String, Long>>();
  }
  @Override
  public Counters.Counter getCounter(Enum<?> name) {
    return null;
  }

  @Override
  public Counters.Counter getCounter(String group, String name) {
    return null;
  }

  public long getCounterValue(String group, String counter) {
    if (!counters.containsKey(group)) {
      return 0L;
    }
    if (!counters.get(group).containsKey(counter)) {
      return 0L;
    }
    return counters.get(group).get(counter);
  }

  @Override
  public InputSplit getInputSplit() {
    return null;
  }

  @Override
  public void incrCounter(Enum<?> key, long amount) {
    // DO nothing
  }

  @Override
  public void incrCounter(String group, String counter, long amount) {
    if (!counters.containsKey(group)) {
      counters.put(group, new HashMap<String, Long>());
    }
    HashMap<String, Long> groupCounters = counters.get(group);

    if (!groupCounters.containsKey(counter)) {
      groupCounters.put(counter, new Long(0));
    }
    Long newValue = groupCounters.get(counter) + amount;
    groupCounters.put(counter, newValue);
  }

  public void clearCounters() {
    counters.clear();
  }

  @Override
  public void setStatus(String status) {
    // Do Nothing
  }

  @Override
  public void progress() {
    //Do nothing
  }
}
