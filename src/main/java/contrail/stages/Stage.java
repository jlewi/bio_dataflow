package contrail.stages;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

/**
 * An abstract base class for each stage of processing.
 *
 * To create a new stage you should overload the following methods
 *   * createParameterDefinitions(): This function returns a map of parameter
 *      definitions which the stage can take. You should always start by calling
 *      the implementation in the base class and adding the result to
 *      a new Map. Its good practice to return an unmodifiable map
 *      by invokig Collections.unmodifiableMap();
 *
 *   * runJob(): This function runs the actual job. You should initialize
 *      the job configuration using the configuration returned by getConf().
 *      This ensures the job uses any generic hadoop options set on the
 *      command line or by the caller depending on how it is run.
 *
 * This class is designed to accommodate running stages in two different ways
 *   1. Directly via the command line.
 *   2. Running the stage from within java.
 *
 * Executing the stage from the command line:
 *   To make the stage executable from the command line you would add a main
 *   function to your subclass. Your main function should use ToolRunner
 *   to invoke run(String[]) so that generic hadoop options get parsed
 *   and added to the configuration.
 *
 * Executing the stage from within java:
 *   Call setOptionValues(Map<String, Object>). The map should pass along
 *   any options required by the stage.
 *
 *   Call runJob(). This runs the job once it has been setup.
 *
 * TODO(jlewi): We should add a function setOptionValuesFromStrings
 * which would set the arguments by parsing the command line options. This
 * would make it more consistent with how we run stages from a binary.
 *
 * TODO(jlewi): Do we need some way to pass along generic hadoop options
 * when running from within java? Since stage implements Configured I think
 * the caller can just set the configuration. runJob should then initialize
 * its job configuration using the configuration stored in the class.
 */
public abstract class Stage extends Configured implements Tool  {
  private static final Logger sLogger =
      Logger.getLogger(Stage.class);
  public Stage() {
  }

  /**
   * A set of key value pairs of options used to configure the stage.
   * These could come from either command line options or previous stages.
   * The data gets passed to the mapper and reducer via the Hadoop
   * Job Configuration.
   */
  protected HashMap<String, Object > stage_options =
      new HashMap<String, Object>();

  /**
   * Definitions of the parameters. Subclasses can access it
   * by calling getParameterDefinitions.
   */
  private Map<String, ParameterDefinition> definitions = null;

  /**
   * Check if the indicated options have been supplied to the stage
   * and if not exit the process printing the help message.
   *
   * @param required: List of required options.
   */
  protected void checkHasParametersOrDie(String[] required) {
    ArrayList<String> missing = new ArrayList<String>();
    for (String arg_name: required) {
      if (!stage_options.containsKey(arg_name) ||
           stage_options.get(arg_name) == null) {
        missing.add(arg_name);
      }
    }

    if (missing.size() > 0) {
      sLogger.error(("Missing required arguments: " +
                     StringUtils.join(missing, ",")));
      printHelp();
      // Should we exit or throw an exception?
      System.exit(0);
    }
  }

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
   * Return a list of the parameter definitions for this stage.
   */
  final public Map<String, ParameterDefinition> getParameterDefinitions() {
    if (definitions == null) {
      definitions = createParameterDefinitions();
    }
    return definitions;
  }

  /**
   * Process the command line options.
   *
   * TODO(jlewi): How should we inform the user of missing arguments?
   */
  protected void parseCommandLine(String[] application_args) {
    // Generic hadoop options should have been parsed out already
    // assuming run(String[]) was invoked via ToolRunner.run.
    // Therefore args should only contain the remaining non generic options.
    // IMPORTANT: Generic options must appear first on the command line i.e
    // before any non generic options.
    Options options = new Options();

    for (Iterator<ParameterDefinition> it =
          getParameterDefinitions().values().iterator(); it.hasNext();) {
      options.addOption(it.next().getOption());
    }
    CommandLineParser parser = new GnuParser();
    CommandLine line;
    try
    {
      line = parser.parse(options, application_args);
      parseCommandLine(line);
    }
    catch( ParseException exp )
    {
      // oops, something went wrong
      System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
      System.exit(1);
    }
  }

  protected void parseCommandLine(CommandLine line) {
    HashMap<String, Object> parameters = new HashMap<String, Object>();

    for (Iterator<ParameterDefinition> it =
            getParameterDefinitions().values().iterator(); it.hasNext();) {
      ParameterDefinition def = it.next();
      Object value = def.parseCommandLine(line);
      if (value != null) {
        parameters.put(def.getName(), value);
      }
    }

    // Set the stage options.
    setParameters(parameters);

    // TODO(jlewi): This is a bit of a hack. We should come up with
    // a better way of handling functionality common to all stages.
    if ((Boolean)stage_options.get("help")) {
      printHelp();
      System.exit(0);
    }
  }

  /**
   * Initialize the hadoop job configuration with information needed for this
   * stage.
   *
   * @param conf: The job configuration.
   */
  protected void initializeJobConfiguration(JobConf conf) {
    // List of options which shouldn't be added to the configuration
    HashSet<String> exclude = new HashSet<String>();
    exclude.add("writeconfig");
    exclude.add("foroozie");
    // Loop over all the stage options and add them to the configuration
    // so that they get passed to the mapper and reducer.
    for (Iterator<String> key_it = stage_options.keySet().iterator();
        key_it.hasNext();) {
      String key = key_it.next();
      if (exclude.contains(key)) {
        continue;
      }
      if (!getParameterDefinitions().containsKey(key)) {
        throw new RuntimeException(
            "Stage:" + this.getClass().getName() + " Doesn't take parameter:" +
            key);
      }
      ParameterDefinition def = getParameterDefinitions().get(key);
      def.addToJobConf(conf, stage_options.get(key));
    }
  }

  /**
   * Print the help message.
   */
  protected void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    Options options = new Options();
    for (Iterator<ParameterDefinition> it =
        getParameterDefinitions().values().iterator(); it.hasNext();) {
      options.addOption(it.next().getOption());
    }
    formatter.printHelp(
        "hadoop jar CONTRAILJAR MAINCLASS [options]", options);
  }

  /**
   * Run the stage.
   */
  protected int run(Map<String, Object> options) throws Exception  {
    // TODO(jlewi): Should this method be public? Is it currently being used?
    // Copy the options from the input.
    stage_options.putAll(options);

    // Run the stage.
    return run();
  }

  /**
   * Run the job.
   *
   * @return
   * @throws Exception
   */
  public RunningJob runJob() throws Exception {
    // TODO(jlewi): runJob should be an abstract method. The only reason its
    // not is because we want to provide backwards compatibility.
    // We should make it abstract as soon as the Stony Brook team has a chance
    // to update their code.
    throw new NotImplementedException("Not implemented");
  }

  /**
   * Run the stage after parsing the string arguments.
   *
   * In general, this implementation should be sufficient and subclasses
   * shouldn't need to overload it.
   *
   * This function should almost never be called directly as it will probably
   * cause problems with the configuration not being set for the configured
   * object.
   */
  public int run(String[] args) throws Exception {
    // TODO(jlewi): Should we check if getConf() returns null and if it does
    // either initialize it or throw an exception. Normally the configuration
    // should be initialized in the caller, e.g in main ToolRunner.run
    // takes a configuration.
    //
    // This function provides the entry point when running from the command
    // line; i.e. using ToolRunner.
    sLogger.info("Tool name: " + this.getClass().getName());
    parseCommandLine(args);
    RunningJob job = runJob();

    if (job == null) {
      // The job doesn't actually run a mapreduce job; e.g when we write
      // job configs.
      return 0;
    }
    if (job.isSuccessful()) {
      return 0;
    } else {
      return -1;
    }
  }

  /**
   * Run the stage.
   * TODO(jlewi): run is deprecated in favor of runJob();
   */
  @Deprecated
  protected int run() throws Exception {
    // We provide a base implementation here because this function is
    // deprecated and we don't subclasses to have to overload it anymore.
    // TODO(jlewi): We could probably just delete this function.
    return 0;
  }

  /**
   * Set the parameters for this stage. Any unset parameters will be
   * initialized to the default values if there is one.
   */
  public void setParameters(Map<String, Object> values) {
    // TODO(jlewi): Should this method be public? Is it currently being used?
    // Copy the options from the input.
    stage_options.putAll(values);

    for (Iterator<ParameterDefinition> it =
            getParameterDefinitions().values().iterator(); it.hasNext();) {
      ParameterDefinition def = it.next();
      // If the value hasn't be set and the parameter has a default value
      // initialize it to the default value
      if (!stage_options.containsKey(def.getName())
          && def.getDefault() != null) {
        stage_options.put(def.getName(), def.getDefault());
      }
    }
  }

  /**
   * Write the job configuration to an XML file specified in the stage option.
   */
  protected void writeJobConfig(JobConf conf) {
    Path jobpath = new Path((String) stage_options.get("writeconfig"));

    // Do some postprocessing of the job configuration before we write it.
    // Overwrite the file if it exists.
    try {
      // We need to use the original configuration because that will have
      // the filesystem.
      FSDataOutputStream writer = jobpath.getFileSystem(conf).create(
          jobpath, true);
      conf.writeXml(writer);
      writer.close();
    } catch (IOException exception) {
      sLogger.error("Exception occured while writing job configuration to:" +
                    jobpath.toString());
      sLogger.error("Exception:" + exception.toString());
    }

    // Post process the configuration to remove properties which shouldn't
    // be specified for oozie.
    // TODO(jlewi): This won't work if the file is on HDFS.
    if (stage_options.containsKey("foroozie")) {
      File xml_file = new File(jobpath.toUri());
      try {
        // Oozie requires certain properties to be specified in the workflow
        // and not in the individual job configuration stages.
        HashSet<String> exclude = new HashSet<String>();
        exclude.add("fs.default.name");
        exclude.add("fs.defaultFS");
        exclude.add("mapred.job.tracker");
        exclude.add("mapred.jar");

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(xml_file);
        NodeList name_nodes = doc.getElementsByTagName("name");
        for (int index = 0; index < name_nodes.getLength(); ++index) {
          Node node = name_nodes.item(index);
          String property_name = node.getTextContent();
          if (exclude.contains(property_name)) {
            Node parent = node.getParentNode();
            Node grand_parent = parent.getParentNode();
            grand_parent.removeChild(parent);
          }
        }
        // write the content into xml file
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(xml_file);
        transformer.transform(source, result);
      } catch (Exception exception) {
        sLogger.error("Exception occured while parsing:" +
		      xml_file.toString());
        sLogger.error("Exception:" + exception.toString());
      }
    }

    sLogger.info("Wrote job config to:" + jobpath.toString());
  }
}

