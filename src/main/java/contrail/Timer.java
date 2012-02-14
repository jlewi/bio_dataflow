package contrail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * Timer class.
 * 
 * This class keeps track acts as an accumulator for run time.
 * You invoke start to start the timer and stop to end the timer.
 * The time in between start and stop is then added to the accumulated
 * time.
 * @author jlewi
 *
 */
public class Timer {

  // String to identify this timer
  private String name = "";
  
  private double start_time;
  
  private long run_time = 0;
  public Timer (String name) {
    this.name = name;
  }

  public void start() {
    start_time=System.currentTimeMillis();
  }
  
  public void stop() { 
    long end_time = System.currentTimeMillis();
    run_time += (end_time - start_time); 
  }
  
  /**
   * Returns the runtime in seconds.
   */
  public double toSeconds() {
    return (run_time / 1000.0);
  }
}
