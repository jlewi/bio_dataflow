package contrail.stages;

import java.util.ArrayList;
import java.util.Collection;
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
   * A list of options that apply to all stages.
   */
  public static List<ParameterDefinition> getCommon() {
    if (stage_options != null) {
      return stage_options;
    }
    stage_options = new ArrayList<ParameterDefinition>();

    ParameterDefinition writeconfig = new ParameterDefinition(
        "writeconfig",
        "The XMLfile to write the job configuration to. The job won't be run.",
        String.class, null);

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


    ParameterDefinition k = new ParameterDefinition(
        "K", "Length of KMers [required].", Integer.class, null);
    stage_options.add(k);
    return stage_options;
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
