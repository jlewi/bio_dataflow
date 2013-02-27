/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Base class for stages
 *
 * This class is a replacement for Stage.
 */
abstract public class StageBase extends Stage {
  private static final Logger sLogger = Logger.getLogger(StageBase.class);

  protected StageInfoWriter infoWriter;

  // The stage if any which launched this stage.
  private StageBase parent;
  /**
   * This function creates the set of parameter definitions for this stage.
   * Overload this function in your subclass to set the definitions for the
   * stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> parameters =
        new HashMap<String, ParameterDefinition>();

    // Return definitions used by all stages.
    for (ParameterDefinition def: ContrailParameters.getCommon()) {
      parameters.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(parameters);
  }

  /**
   * Returns a list of the required parameters.
   *
   * Ideally subclasses shouldn't need to overload this.
   */
  protected List<String> getRequiredParameters() {
    ArrayList<String> required = new ArrayList<String>();

    // Parameters with no default value are assumed to be required.
    for (ParameterDefinition def : getParameterDefinitions().values()) {
      if (def.getDefault() == null) {
        required.add(def.getName());
      }
    }

    return required;
  }


  /**
   * Initialize the stage by inheriting the settings from other.
   *
   * This function sets up this stage as a child of other.
   *
   * This function will throw an error if any of the settings have already
   * been set.
   *
   * @param other
   */
  public void initializeAsChild(StageBase other) {
    parent = other;
    // Check if any of the settings have already been set.
    if (stage_options.size() != 0) {
      sLogger.fatal(
          "This stage already has parameters set so it can't be initialized.",
          new RuntimeException("Already initialitized"));
    }

    if (getConf() != null) {
      sLogger.fatal(
          "This stage already has a hadoop configuration.",
          new RuntimeException("Already initialitized"));
    }

    // Initialize the hadoop configuration so we inherit hadoop variables
    // like number of map tasks.
    setConf(other.getConf());

    // Get the parameters.
    stage_options.putAll(ContrailParameters.extractParameters(
        other.stage_options, getParameterDefinitions().values()));

    infoWriter = other.infoWriter;
  }

  /**
   * A class containing information about invalid parameters.
   */
  public class InvalidParameter {
    public String stage;
    // Name of the invalid parameter.
    final public String name;

    // Message describing why the parameter is invalid.
    final public String message;

    public InvalidParameter(String name, String message) {
      this.name = name;
      this.message = message;
    }
  }

  /**
   * Returns information about the entire workflow that this stage belongs to.
   */
  public StageInfo getWorkflowInfo() {
    // Find the root of the tree
    StageBase root = this;
    while (root.parent != null) {
      root = root.parent;
    }

    return root.getStageInfo();
  }

  /**
   * Returns the info about just this stage.
   *
   * @return
   */
  abstract public StageInfo getStageInfo();

  /**
   * Run the stage.
   */
  abstract public boolean execute();

  /**
   * Set the parameter.
   * @param name
   * @param value
   */
  public void setParameter(String name, Object value) {
    stage_options.put(name, value);
  }

  /**
   * Check whether parameters are valid.
   * Subclasses which override this method should call the base class
   *
   * We return information describing all the invalid parameters. If
   * the validation requires access to a valid job configuration
   * then the caller should ensure the configuration is properly set.
   */
  public List<InvalidParameter> validateParameters() {
    // TODO(jeremy@lewi.us): Should we automatically check that required
    // parameters are set. The question is whether a parameter which has
    // null for the default value should be considered required?
    return new ArrayList<InvalidParameter>();
  }

  /**
   * Run the job.
   *
   * @return
   * @throws Exception
   */
  @Deprecated
  final public RunningJob runJob() throws Exception {
    // TODO(jeremy@lewi.us): Get rid of this method as soon as we get
    // rid of stage.
    throw new RuntimeException("No longer supported");
  }

  /**
   * Run the stage.
   * TODO(jlewi): run is deprecated in favor of runJob();
   */
  @Deprecated
  final protected int run() throws Exception {
    // TODO(jeremy@lewi.us): Get rid of this method as soon as we get
    // rid of stage.
    throw new RuntimeException("No longer supported");
  }

  protected void setupLogging() {
    String logFile = (String) stage_options.get("log_file");

    if (logFile != null && logFile.length() > 0) {
      boolean hasAppender = false;

      for (Enumeration<Appender> e = Logger.getRootLogger().getAllAppenders();
           e.hasMoreElements(); ) {
        Appender logger = e.nextElement();
        if (logger.getName().equals(logFile)) {
          // We've already setup the logger to the file so we don't setup
          // another one because that would cause messages to be logged multiple
          // times.
          return;
        }
      }
      FileAppender fileAppender = new FileAppender();
      fileAppender.setFile(logFile);
      PatternLayout layout = new PatternLayout();
      layout.setConversionPattern("%d{ISO8601} %p %c: %m%n");
      fileAppender.setLayout(layout);
      fileAppender.activateOptions();

      // Name the appender based on the file to write to so that we can
      // check whether this appender has already been added.
      fileAppender.setName(logFile);
      Logger.getRootLogger().addAppender(fileAppender);
      sLogger.info("Start logging");
      sLogger.info("Adding a file log appender to: " + logFile);
    }
  }

  /**
   * Return information about the stage.
   *
   * @param job
   * @return
   */
  @Deprecated
  final public StageInfo getStageInfo(RunningJob job) {
    // TODO(jeremy@lewi.us): Get rid of this method as soon as we get
    // rid of stage.
    throw new RuntimeException("No longer supported");
  }
}
