package contrail.tools;

import java.util.List;

import org.apache.hadoop.mapred.JobConf;

import contrail.stages.NotImplementedException;
import contrail.stages.StageBase;
import contrail.stages.StageInfo;
import contrail.util.ContrailLogger;

/**
 * Base class for filters.
 *
 * We inherit from StageBase to get all the parameter parsing routines.
 */
abstract public class FilterBase extends StageBase {
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      FilterBase.class);
  /**
   * Separators used for options.
   */
  final public static String OptionSeparator = ";";
  /**
   * Parse a line containing parameters and add them to the stage.
   *
   * @param line: Comma separated list of key value pairs.
   */
  public void addParameters(String line) {
    String[] args = line.split(OptionSeparator);
    this.parseCommandLine(args);
  }

  /**
   * Validate Parameters.
   *
   * TODO(jlewi): Should we return errors rather than dying.
   */
  public void validateParametersOrDie() {
    checkHasParametersOrDie(getRequiredParameters().toArray(new String[]{}));
    setDefaultParameters();
    List<InvalidParameter> invalidParameters = super.validateParameters();

    if (invalidParameters.size() > 0) {
      for (InvalidParameter parameter : invalidParameters) {
        sLogger.fatal(
            String.format(
                "Parameter: %s isn't valid: %s",
                parameter.name, parameter.message));
      }
      sLogger.fatal("Parameters are invalid.", new IllegalArgumentException());
      System.exit(-1);
    }
  }

  public void addParametersToJobConf(JobConf jobconf) {
    initializeJobConfiguration(jobconf);
  }

  /**
   * Return the class definition for the mapper which this filter uses.
   * @return
   */
  abstract public Class filterClass();

  // These methods are inherited from StageBase but should not be used.
  @Override
  final public int run(String[] args) throws Exception {
    throw new NotImplementedException();
  }

  @Override
  final public StageInfo getStageInfo() {
    throw new NotImplementedException();
  }

  @Override
  final public boolean execute() {
    throw new NotImplementedException();
  }
}
