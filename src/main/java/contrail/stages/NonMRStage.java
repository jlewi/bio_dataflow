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
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

/**
 * Stage with hooks for customizing different parts of execution for a non
 * MR stage.
 *
 * This is an experimental class. The goal of this class is to try to
 * address some of the pain points of using Stage. Currently the stage
 * class requires the subclass writers to manually do repetitive tasks
 * such as initialize the hadoop configuration, validate parameters etc...
 *
 * The point of this class is to handle all the common processing but
 * provide hooks at various points in stage execution which can be overloaded
 * to allow the subclass to customize functionality at different points.
 */
public abstract class NonMRStage extends StageBase {
  private static final Logger sLogger = Logger.getLogger(NonMRStage.class);

  protected StageState stageState;

  public NonMRStage() {
    infoWriter = null;
  }

  /**
   * Hook which runs after the job runs.
   *
   * This hook will be called regardless of whether the job is successful.
   */
  protected void postRunHook() {
    // Do nothing by default.
  }

  /**
   * Code for the stage goes here.
   */
  abstract protected void stageMain();

  /**
   * Return information about the stage.
   *
   * @param job
   * @return
   */
  public StageInfo getStageInfo() {
    StageInfo info = new StageInfo();
    info.setCounters(new ArrayList<CounterInfo>());
    info.setParameters(new ArrayList<StageParameter>());
    info.setModifiedParameters(new ArrayList<StageParameter>());
    info.setSubStages(new ArrayList<StageInfo>());
    info.setStageClass(this.getClass().getName());
    info.setState(stageState);
    ArrayList<String> keys = new ArrayList<String>();
    keys.addAll(stage_options.keySet());
    Collections.sort(keys);

    for (String key : keys) {
      StageParameter parameter = new StageParameter();
      parameter.setName(key);
      parameter.setValue(stage_options.get(key).toString());
      info.getParameters().add(parameter);
    }

    // TODO(jlewi): We should keep track of the state somehow.
    return info;
  }

  /**
   * Execute the stage.
   *
   * @return: True on success false otherwise.
   */
  final public boolean execute() {
    setupLogging();
    checkHasParametersOrDie(getRequiredParameters().toArray(new String[]{}));
    List<InvalidParameter> invalidParameters = validateParameters();

    stageState = StageState.STARTED;
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

    // Initialize the hadoop configuration.
    if (getConf() == null) {
      setConf(new JobConf(this.getClass()));
    } else {
      setConf(new JobConf(getConf(), this.getClass()));
    }
    JobConf conf = (JobConf) getConf();
    initializeJobConfiguration(conf);

    logParameters();

    // Write the stageinfo if a writer is specified.
    if (infoWriter != null) {
      infoWriter.write(getWorkflowInfo());
    }

    stageMain();

    // TODO(jeremy@lewi.us): How to signify failure?
    stageState = StageState.SUCCESS;

    if (infoWriter != null) {
      infoWriter.write(getWorkflowInfo());
    }

    return true;
  }

  /**
   * Run the stage after parsing the string arguments.
   */
  final public int run(String[] args) throws Exception {
    //
    // This function provides the entry point when running from the command
    // line; i.e. using ToolRunner.
    sLogger.info("Tool name: " + this.getClass().getName());

    // Print the command line on a single line as its convenient for
    // copy pasting.
    sLogger.info("Command line arguments: " + StringUtils.join(args, " "));

    parseCommandLine(args);
    if (execute()) {
      return 0;
    } else {
      return -1;
    }
  }
}
