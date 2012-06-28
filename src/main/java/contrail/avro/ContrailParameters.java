package contrail.avro;

import java.util.ArrayList;
import java.util.List;

/* This class encapsulates the common parameter definitions.
 *
 * If a parameter is used by more than 1 stage its definition should go
 * here. Otherwise its definition should go in the stage where it is used.
 */
public class ContrailParameters {
//  private static List<Option> help_options;
//
//  /**
//   * Returns a list of options for getting help
//   * @return
//   */
//  public static List<Option> getHelpOptions() {
//    if (help_options != null) {
//      return help_options;
//    }
//    help_options = new ArrayList<Option>();
//
//    help_options.add(new Option("help", "print this message"));
//    help_options.add(new Option("h", "print this message"));
//
//    return help_options;
//  }


//  private static List<Option> hadoop_options;
//  /**
//   * Get options related to hadoop.
//   */
//  public static List<Option> getHadoopOptions() {
//    if (hadoop_options != null) {
//      return hadoop_options;
//    }
//    hadoop_options = new ArrayList<Option>();
//
//    // Default values.
//    // hadoop options
//    int    HADOOP_MAPPERS    = 50;
//    int    HADOOP_REDUCERS   = 50;
//    int    HADOOP_LOCALNODES = 1000;
//    long   HADOOP_TIMEOUT    = 3600000;
//    String HADOOP_JAVAOPTS   = "-Xmx1000m";
//    String localBasePath = "work";
//
//    // work directories
//    hadoop_options.add(
//        OptionBuilder.withArgName("hadoopBasePath").hasArg().withDescription(
//            "Base Hadoop assembly directory [required]").create("asm"));
//    hadoop_options.add(OptionBuilder.withArgName(
//        "hadoopReadPath").hasArg().withDescription(
//            "Hadoop read directory [required]").create("reads"));
//
//    hadoop_options.add(OptionBuilder.withArgName(
//        "workdir").hasArg().withDescription(
//            "Local work directory (default: " + localBasePath + ")").create(
//                "work"));
//
//    // hadoop options
//    hadoop_options.add(OptionBuilder.withArgName(
//        "numSlots").hasArg().withDescription(
//            "Number of machine slots to use (default: " +
//            HADOOP_MAPPERS + ")").create("slots"));
//    hadoop_options.add(OptionBuilder.withArgName("numNodes").hasArg().withDescription("Max nodes in memory (default: " + HADOOP_LOCALNODES + ")").create("nodes"));
//    hadoop_options.add(OptionBuilder.withArgName("childOpts").hasArg().withDescription("Child Java Options (default: " + HADOOP_JAVAOPTS + ")").create("javaopts"));
//    hadoop_options.add(OptionBuilder.withArgName("millisecs").hasArg().withDescription("Hadoop task timeout (default: " + HADOOP_TIMEOUT + ")").create("timeout"));
//
//    return hadoop_options;
//  }

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
}
