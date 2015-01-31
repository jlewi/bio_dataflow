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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* This class encapsulates the common parameter definitions.
 *
 * If a parameter is used by more than 1 stage its definition should go
 * here. Otherwise its definition should go in the stage where it is used.
 */
public class ContrailParameters {
  private static List<ParameterDefinition> path_options;

  /**
   * Get options defining input and output path.
   */
  public static List<ParameterDefinition> getInputOutputPathOptions() {
    // We define the options for input and output paths globally
    // because we want to be consistent when specifying the options
    // for running stages independently.
    if (path_options != null) {
      return path_options;
    }
    path_options = new ArrayList<ParameterDefinition>();

    ParameterDefinition input = new ParameterDefinition(
        "inputpath", "The directory containing the input",
        String.class,
        null);
    ParameterDefinition output = new ParameterDefinition(
        "outputpath", "The directory where the output should be written to.",
        String.class, null);

    path_options.add(input);
    path_options.add(output);
    return path_options;
  }

  private static List<ParameterDefinition> stage_options;

  /**
   * Return the parameter which
   * @return
   */
  public static ParameterDefinition getK() {
    ParameterDefinition k = new ParameterDefinition(
        "K", "Length of KMers [required].", Integer.class, null);
    return k;
  }

  /**
   * A list of options that apply to all stages.
   *
   * TODO(jeremy@lewi.us): This is deprecated because K shouldn't be
   * a parameter that applies to all stages.
   * Common parameters should just be initialized in
   * Stage.createParameterDefinitions. Stages which require K should
   * add it explicitly.
   */
  @Deprecated
  public static List<ParameterDefinition> getCommon() {
    if (stage_options != null) {
      return stage_options;
    }
    stage_options = new ArrayList<ParameterDefinition>();

    ParameterDefinition writeconfig = new ParameterDefinition(
        "writeconfig",
        "The XMLfile to write the job configuration to. The job won't be run.",
        String.class, "");

    stage_options.add(writeconfig);

    ParameterDefinition foroozie = new ParameterDefinition(
        "foroozie",
        "If writeconfig is also specified, then the job configuration is " +
        "post-processed to make it suitable for use with oozie.",
        Boolean.class, false);

    stage_options.add(foroozie);

    ParameterDefinition help = new ParameterDefinition(
        "help", "Print this help message.", Boolean.class, false);
    help.setShortName("h");
    stage_options.add(help);

    ParameterDefinition logFile = new ParameterDefinition(
        "log_file", "File to log to. This can't be an HDFS path.", String.class,
        "");
    stage_options.add(logFile);
    stage_options = Collections.unmodifiableList(stage_options);
    return stage_options;
  }

  /**
   * The option computing the graph stats.
   */
  public static ParameterDefinition getComputeStats() {
    ParameterDefinition stats = new ParameterDefinition(
        "compute_stats", "If true compute statistics about the graph after " +
        "each stage [optional].", Boolean.class, false);

    return stats;
  }

  /**
   * The option controlling cleanup.
   */
  public static ParameterDefinition getCleanup() {
    ParameterDefinition cleanup = new ParameterDefinition(
        "cleanup", "If true(default) temporary directories are aggressively " +
        "cleaned up. For debugging it can be useful to keep the intermediate " +
        "directories by setting this to false. [optional].", Boolean.class,
        true);

    return cleanup;
  }

  /**
   * Add a definition of parameters to a map of parameters.
   * @param map
   * @param parameters
   */
  public static void add(
      Map<String, ParameterDefinition> map, ParameterDefinition definition) {
    map.put(definition.getName(),definition);
  }

  /**
   * Add a list of parameters to a map of parameters.
   * @param map
   * @param parameters
   */
  public static void addList(
      HashMap<String, ParameterDefinition> map,
      List<ParameterDefinition> parameters) {
    for (ParameterDefinition param: parameters) {
      map.put(param.getName(), param);
    }
  }

  /**
   * Returns a shallow copy of those parameters in options which stage takes.
   * @param options
   * @param stage
   * @return
   */
  public static Map<String, Object> extractParameters(
      Map<String, Object> options,
      Collection<ParameterDefinition> definitions) {
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> substage_options = new HashMap<String, Object> ();
    for (ParameterDefinition def: definitions) {
      if (options.containsKey(def.getName())) {
        substage_options.put(def.getName(), options.get(def.getName()));
      }
    }
    return substage_options;
  }

}
