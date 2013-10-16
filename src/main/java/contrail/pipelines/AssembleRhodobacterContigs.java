package contrail.pipelines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Assemble contigs for the rhodobacter dataset.
 *
 * The purpose of this class is simply to encapsulate a set of good parameters
 * for this dataset.
 */
public class AssembleRhodobacterContigs extends AssembleContigs {
  private static final Logger sLogger = Logger.getLogger(
      AssembleRhodobacterContigs.class);

  public AssembleRhodobacterContigs() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }

  /**
   * Set the default parameters for the staph dataset.
   */
  @Override
  protected void setDefaultParameters() {
    // Set any parameters to the default value if they haven't already been
    // set.
    if (!stage_options.containsKey("K")) {
      stage_options.put("K", new Integer(51));
    }
    if (!stage_options.containsKey("tiplength")) {
      stage_options.put("tiplength", new Integer(110));
    }
    if (!stage_options.containsKey("bubble_edit_rate")) {
      stage_options.put("bubble_edit_rate", new Float(.05f));
    }
    if (!stage_options.containsKey("bubble_length_threshold")) {
      stage_options.put("bubble_length_threshold", new Integer(110));
    }
    if (!stage_options.containsKey("length_thresh")) {
      stage_options.put("length_thresh", new Integer(110));
    }
    if (!stage_options.containsKey("low_cov_thresh")) {
      stage_options.put("low_cov_thresh", new Float(5.0f));
    }
    // We don't record the reads because this information isn't used anywhere.
    if (!stage_options.containsKey("MAXTHREADREADS")) {
      stage_options.put("MAXTHREADREADS", new Integer(0));
    }

    // Call the base class to set any remaining parameters which haven't been
    // initialized. We do this at the end of this function because we want
    // the defaults set here to take precedence over the defaults in the base
    // class.
    super.setDefaultParameters();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AssembleRhodobacterContigs(), args);
    System.exit(res);
  }
}
