package contrail.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

/**
 * A class for containing command line option definitions for options shared
 * by multiple stages. 
 * 
 * This class is currently a bit experimental.
 */
public class ContrailOptions {

  private static List<Option> help_options;
  /**
   * Returns a list of options for getting help
   * @return
   */
  public static List<Option> getHelpOptions() {
    if (help_options != null) {
      return help_options;
    }
    help_options = new ArrayList<Option>();
    
    help_options.add(new Option("help", "print this message"));
    help_options.add(new Option("h", "print this message"));
    
    return help_options;
  }
  
  
  private static List<Option> hadoop_options;
  /**
   * Get options related to hadoop.
   */
  public static List<Option> getHadoopOptions() {
    if (hadoop_options != null) {
      return hadoop_options;
    }
    hadoop_options = new ArrayList<Option>();
    
    // Default values.
    // hadoop options
    int    HADOOP_MAPPERS    = 50;
    int    HADOOP_REDUCERS   = 50;
    int    HADOOP_LOCALNODES = 1000;
    long   HADOOP_TIMEOUT    = 3600000;
    String HADOOP_JAVAOPTS   = "-Xmx1000m";
    String localBasePath = "work";
    
    // work directories
    hadoop_options.add(
        OptionBuilder.withArgName("hadoopBasePath").hasArg().withDescription(
            "Base Hadoop assembly directory [required]").create("asm"));
    hadoop_options.add(OptionBuilder.withArgName(
        "hadoopReadPath").hasArg().withDescription(
            "Hadoop read directory [required]").create("reads"));
    
    hadoop_options.add(OptionBuilder.withArgName(
        "workdir").hasArg().withDescription(
            "Local work directory (default: " + localBasePath + ")").create(
                "work"));
  
    // hadoop options
    hadoop_options.add(OptionBuilder.withArgName(
        "numSlots").hasArg().withDescription(
            "Number of machine slots to use (default: " +
            HADOOP_MAPPERS + ")").create("slots"));
    hadoop_options.add(OptionBuilder.withArgName("numNodes").hasArg().withDescription("Max nodes in memory (default: " + HADOOP_LOCALNODES + ")").create("nodes"));
    hadoop_options.add(OptionBuilder.withArgName("childOpts").hasArg().withDescription("Child Java Options (default: " + HADOOP_JAVAOPTS + ")").create("javaopts"));
    hadoop_options.add(OptionBuilder.withArgName("millisecs").hasArg().withDescription("Hadoop task timeout (default: " + HADOOP_TIMEOUT + ")").create("timeout"));
    
    return hadoop_options;
  }
  
  private static List<Option> path_options;
  
  /**
   * Get options defining input and output path.  
   */
  public static List<Option> getInputOutputPathOptions() {
    // We define the options for input and output paths globally
    // because we want to be consistent when specifying the options
    // for running stages independently.
    if (path_options != null) {
      return path_options;
    }
    path_options = new ArrayList<Option>();
    
    Option input = new Option("inputpath", "inputpath", true,
        "The directory containing the input (i.e the output of BuildGraph.)");
    Option output = new Option("outputpath", "outputpath", true,
        "The directory where the output should be written to.");

    path_options.add(input);
    path_options.add(output);
    return path_options;
  }
  
  private static List<Option> stage_options;
  
  /**
   * A list of options that apply to all stages.
   */
  public static List<Option> getStageOptions() {
    // We define the options for input and output paths globally
    // because we want to be consistent when specifying the options
    // for running stages independently.
    if (stage_options != null) {
      return stage_options;
    }
    stage_options = new ArrayList<Option>();
    
    Option writeconfig = new Option("writeconfig", "writeconfig", true,
        "The XMLfile to write the job configuration to. The job won't be run.");

    stage_options.add(writeconfig);
    return stage_options;
  }
}
