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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

import contrail.util.ContrailLogger;

/**
 * Stage with hooks for customizing different parts of an MR execution.
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
public class MRStage extends StageBase {
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      MRStage.class);
  protected RunningJob job;

  public MRStage() {
    job = null;
    infoWriter = null;
  }

  /**
   * Subclasses should override this hook and use it to configure the job.
   *
   * For example, the subclass should set the input/output format for the
   * configuration.
   */
  protected void setupConfHook() {
    // Do nothing by default.
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
   * Return information about the stage.
   *
   * @param job
   * @return
   */
  @Override
  public StageInfo getStageInfo() {
    StageInfo info = new StageInfo();
    info.setCounters(new ArrayList<CounterInfo>());
    info.setParameters(new ArrayList<StageParameter>());
    info.setModifiedParameters(new ArrayList<StageParameter>());
    info.setSubStages(new ArrayList<StageInfo>());

    info.setStageClass(this.getClass().getName());

    ArrayList<String> keys = new ArrayList<String>();
    keys.addAll(stage_options.keySet());
    Collections.sort(keys);

    for (String key : keys) {
      StageParameter parameter = new StageParameter();
      parameter.setName(key);
      parameter.setValue(stage_options.get(key).toString());
      info.getParameters().add(parameter);
    }

    if (job != null) {
      try {
        if (job.isSuccessful()) {
          info.setState(StageState.SUCCESS);
        } else {
          info.setState(StageState.ERROR);
        }
        Counters counters = job.getCounters();
        Iterator<Group> itGroup = counters.iterator();
        while (itGroup.hasNext()) {
          Group group = itGroup.next();
          Iterator<Counters.Counter> itCounter = group.iterator();
          while (itCounter.hasNext()) {
            Counters.Counter counter = itCounter.next();
            CounterInfo counterInfo = new CounterInfo();
            counterInfo.setName(counter.getDisplayName());
            counterInfo.setValue(counter.getValue());
            info.getCounters().add(counterInfo);
          }
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        sLogger.fatal("Couldn't get stage counters", e);
        System.exit(-1);
      }
    } else {
      info.setState(StageState.STARTED);
    }
    return info;
  }

  /**
   * Execute the stage.
   *
   * @return: True on success false otherwise.
   */
  @Override
  final public boolean execute() {
    setupLogging();
    checkHasParametersOrDie(getRequiredParameters().toArray(new String[]{}));
    setDefaultParameters();
    List<InvalidParameter> invalidParameters = validateParameters();

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
    conf.setJobName(this.getClass().getSimpleName());
    initializeJobConfiguration(conf);
    setupInfoWriter();
    setupConfHook();
    logParameters();
    if (stage_options.containsKey("writeconfig") &&
        ((String)stage_options.get("writeconfig")).length() >0) {
      writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already.
      // TODO(jlewi): We should add an option to disable this so we don't
      // accidentally override data.
      Path outPath = FileOutputFormat.getOutputPath(conf);
      if (outPath == null) {
        sLogger.fatal(
            "No output path is set in the job configuration. Did you set it " +
            "in setupConfHook()?");
        System.exit(-1);
      }
      try {
        FileSystem outFs = outPath.getFileSystem(conf);
        if (outFs.exists(outPath)) {
          // TODO(jlewi): We should only delete an existing directory
          // if explicitly told to do so.
          sLogger.info("Deleting output path: " + outPath.toString() + " "
              + "because it already exists.");
          outFs.delete(outPath, true);
        }
      } catch (IOException e) {
        sLogger.fatal(
            "There was a problem checking if the directory:" +
            outPath.toString() + " exists and deleting it if it does.", e);
        System.exit(-1);
      }
      try {
        // Write the stageinfo if a writer is specified.
        if (infoWriter != null) {
          infoWriter.write(getWorkflowInfo());
        }
        job = JobClient.runJob(conf);
        if (infoWriter != null) {
          infoWriter.write(getWorkflowInfo());
        }
        postRunHook();
        return job.isSuccessful();
      } catch (IOException e) {
        if (infoWriter != null) {
          infoWriter.write(getWorkflowInfo());
        }
        sLogger.fatal(
            "There was a problem running the mr job.", e);
        System.exit(-1);
      }
    }

    return true;
  }

  /**
   * Return the number of input records to the mapper.
   * @return: If the job hasn't run returns -1.
   */
  public long getNumMapInputRecords() {
    try {
      if (!job.isSuccessful()) {
        return -1;
      }
      return job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "MAP_INPUT_RECORDS").getValue();

    } catch (IOException e) {
      sLogger.fatal("Couldn't get job state.", e);
      System.exit(-1);
    }
    return -1;
  }

  /**
   * Return the number of reduce output records.
   * @return: If the job hasn't run returns -1.
   */
  public long getNumReduceOutputRecords() {
    try {
      if (!job.isSuccessful()) {
        return -1;
      }
      return job.getCounters().findCounter(
          "org.apache.hadoop.mapred.Task$Counter",
          "REDUCE_OUTPUT_RECORDS").getValue();

    } catch (IOException e) {
      sLogger.fatal("Couldn't get job state.", e);
      System.exit(-1);
    }
    return -1;
  }

  /**
   * Return the specified counter.
   *
   * @param group
   * @param name
   * @return
   */
  public long getCounter(String group, String name) {
    try {
      if (!job.isSuccessful()) {
        return -1;
      }
      return job.getCounters().findCounter(
          group, name).getValue();

    } catch (IOException e) {
      sLogger.fatal("Couldn't get job state.", e);
      System.exit(-1);
    }
    return -1;
  }

  /**
   * Run the stage after parsing the string arguments.
   */
  @Override
  final public int run(String[] args) throws Exception {
    //
    // This function provides the entry point when running from the command
    // line; i.e. using ToolRunner.
    sLogger.info("Tool name: " + this.getClass().getName());

    // Print the command line on a single line as its convenient for
    // copy pasting.
    sLogger.info("Command line arguments: " + StringUtils.join(args, " "));

    parseCommandLine(args);
    execute();
    if (job==null || !job.isSuccessful()) {
      return -1;
    } else {
      return 0;
    }
  }
}
