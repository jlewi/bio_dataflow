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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import contrail.util.FileHelper;

/**
 * An abstract base class for each stage of processing.
 *
 * To create a new stage you should overload the following methods
 *   * createParameterDefinitions(): This function returns a map of parameter
 *      definitions which the stage can take. You should always start by calling
 *      the implementation in the base class and adding the result to
 *      a new Map. Its good practice to return an unmodifiable map
 *      by invoking Collections.unmodifiableMap();
 *
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
 *
 */
abstract public class StageBase extends Configured implements Tool {
  private static final Logger sLogger = Logger.getLogger(StageBase.class);

  protected StageInfoWriter infoWriter;

  // The stage if any which launched this stage.
  private StageBase parent;

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

    ParameterDefinition stageInfoPath = new ParameterDefinition(
        "stageinfopath", "The directory to write stage info to. Default is " +
        "a subdirectory of the outputpath.", String.class, "");

    parameters.put(stageInfoPath.getName(), stageInfoPath);
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
   * Set the default value for any parameters which aren't set and for which
   * a default is supplied.
   */
  protected void setDefaultParameters() {
    for (Iterator<ParameterDefinition> it =
         getParameterDefinitions().values().iterator(); it.hasNext();) {
      ParameterDefinition def = it.next();
      // If the value hasn't been set and the parameter has a default value
      // initialize it to the default value
      if (!stage_options.containsKey(def.getName())
          && def.getDefault() != null) {
        stage_options.put(def.getName(), def.getDefault());
      }
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
    setDefaultParameters();
    // TODO(jlewi): This is a bit of a hack. We should come up with
    // a better way of handling functionality common to all stages.
    if ((Boolean)stage_options.get("help")) {
      printHelp();
      System.exit(0);
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
   * Set the parameters for this stage. Any parameters which haven't been
   * previously set and which aren't part of the input will be
   * initialized to the default values if there is one.
   */
  public void setParameters(Map<String, Object> values) {
    // TODO(jlewi): Should this method be public? Is it currently being used?
    // Copy the options from the input.
    stage_options.putAll(values);

    if (!(this instanceof StageBase)) {
      // TODO(jeremy@lewi.us): For backwards compatibility with classes
      // which are subclasses of Stage and not StageBase. if WriteConfig
      // is the empty string we need to remove it as a parameter.
      // This is because the old code assumed that if writeconfig isn't true
      // it won't be a parameter.
      String value = (String) stage_options.get("writeconfig");
      if (value.length() == 0) {
        stage_options.remove("writeconfig");
      }
    }
  }

  /**
   * Helper function for validateParameters. Checks that all of the parameters
   * with the supplied names have non empty string values.
   *
   * @param names
   * @return: A list of invalid parameters.
   */
  protected List<InvalidParameter> checkParameterIsNonEmptyString(
      Collection<String> names) {
    ArrayList<InvalidParameter> invalid = new ArrayList<InvalidParameter>();
    for (String name : names) {
      if (!stage_options.containsKey(name)) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Missing parameter %s.", name));
        invalid.add(parameter);
        continue;
      }
      String value = (String) stage_options.get(name);
      if (value.length() == 0) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Parameter %s is the empty string but it " +
        "must be a non null value.", name));
        invalid.add(parameter);
        continue;
      }
    }
    return invalid;
  }

  /**
   * Helper function for validateParameters. Checks that all of the parameters
   * with the supplied names are supplied and that the value points to
   * an existing file on the local filesystem.
   *
   * @param names
   * @return: A list of invalid parameters.
   */
  protected List<InvalidParameter> checkParameterIsExistingLocalFile(
      Collection<String> names) {
    ArrayList<InvalidParameter> invalid = new ArrayList<InvalidParameter>();
    for (String name : names) {
      if (!stage_options.containsKey(name)) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Missing parameter %s.", name));
        invalid.add(parameter);
        continue;
      }
      String value = (String) stage_options.get(name);
      if (value.length() == 0) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Parameter %s is the empty string but it " +
        "must be an existing file on the local filesystem.", name));
        invalid.add(parameter);
        continue;
      }

      File file = new File(value);
      if (!(file.exists())) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format(
            "The value of name is %s which is not an existing file on the " +
            "local filesystem.", value));
        invalid.add(parameter);
        continue;
      }
    }
    return invalid;
  }

  /**
   * Helper function for validateParameters. Checks that all of the parameters
   * with the supplied names are supplied and that the value is a glob which
   * matches at least one file on the local filesystem.
   *
   * @param names
   * @return: A list of invalid parameters.
   */
  protected List<InvalidParameter> checkParameterMatchesLocalFiles(
      Collection<String> names) {
    ArrayList<InvalidParameter> invalid = new ArrayList<InvalidParameter>();
    for (String name : names) {
      if (!stage_options.containsKey(name)) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Missing parameter %s.", name));
        invalid.add(parameter);
        continue;
      }
      String value = (String) stage_options.get(name);
      if (value.length() == 0) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Parameter %s is the empty string but it " +
        "must be a glob matching files on the local filesystem.", name));
        invalid.add(parameter);
        continue;
      }

      // Make sure reads glob matched some files.
      ArrayList<String> matchedFles = FileHelper.matchListOfGlobs(value);

      if (matchedFles.isEmpty()) {
        InvalidParameter parameter = new InvalidParameter(
            name, "No files matched: "  +
                (String) this.stage_options.get(name));

        invalid.add(parameter);
        continue;
      }
    }
    return invalid;
  }

  /**
   * Helper function for validateParameters. Checks that all of the parameters
   * with the supplied names are supplied and that the value is a glob or
   * directory which matches at least one file. The globs can match distributed
   * filesystems or the local filesystem.
   *
   * @param names
   * @return: A list of invalid parameters.
   */
  protected List<InvalidParameter> checkParameterMatchesFiles(
      Collection<String> names) {
    ArrayList<InvalidParameter> invalid = new ArrayList<InvalidParameter>();
    for (String name : names) {
      if (!stage_options.containsKey(name)) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Missing parameter %s.", name));
        invalid.add(parameter);
        continue;
      }
      String value = (String) stage_options.get(name);
      if (value.length() == 0) {
        InvalidParameter parameter = new InvalidParameter(
            name, String.format("Parameter %s is the empty string but it " +
        "must be a glob matching files on the local filesystem.", name));
        invalid.add(parameter);
        continue;
      }

      // Make sure reads glob matched some files.
      ArrayList<Path> matchedFles = FileHelper.matchListOfGlobsWithDefault(
          getConf(), value, "*");

      if (matchedFles.isEmpty()) {
        InvalidParameter parameter = new InvalidParameter(
            name, "No files matched: "  +
                (String) this.stage_options.get(name));

        invalid.add(parameter);
        continue;
      }
    }
    return invalid;
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
      sLogger.error(
          this.getClass().getSimpleName() +": Missing required " +
          "arguments: " + StringUtils.join(missing, ","));
      printHelp();
      // Should we exit or throw an exception?
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

  protected void setupLogging() {
    String logFile = (String) stage_options.get("log_file");

    if (logFile != null && logFile.length() > 0) {
      boolean hasAppender = false;

      for (Enumeration<Appender> e = Logger.getRootLogger().getAllAppenders();
           e.hasMoreElements(); ) {
        Appender logger = e.nextElement();
        if (logger.getName() != null && logger.getName().equals(logFile)) {
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
   * Setup the writer for stage information.
   */
  protected void setupInfoWriter() {
    if (infoWriter != null ) {
      // Its already setup.
      return;
    }

    if (getConf() == null) {
      // We require a valid hadoop configuration.
      sLogger.fatal("Can't setup InfoWriter because configuraiton is null.");
    }

    // Setup the stageInfo writer if we haven't already.
    String stageInfoPath = (String) (stage_options.get("stageinfopath"));
    if (stageInfoPath.isEmpty()) {
      if (stage_options.containsKey("outputpath")) {
        String outputPath = (String) stage_options.get("outputpath");
        // We don't make stage_info a subdirectory of outputpath by default
        // because Hadoop will complain if the output directory already exists.
        stageInfoPath = outputPath + ".stage_info";
      }
    }

    if (!stageInfoPath.isEmpty()) {
      infoWriter = new StageInfoWriter(getConf(), stageInfoPath);
      sLogger.info("Stage info will be written to:" + stageInfoPath);
    } else {
      sLogger.info(
          "No stage info will be written because no outputpath for this " +
          "stage.");
    }
  }

  /**
   * This function logs the values of the options.
   */
  protected void logParameters() {
    ArrayList<String> keys = new ArrayList<String>();
    keys.addAll(stage_options.keySet());
    Collections.sort(keys);
    ArrayList<String> commandLine = new ArrayList<String>();
    for (String key : keys) {
      commandLine.add(String.format(
          "--%s=%s", key, stage_options.get(key).toString()));
    }
    // Print out all the parameters on one line. This is convenient
    // for copying and pasting to rerun the stage.
    sLogger.info(String.format(
        "%s: Parameters: %s", this.getClass().getSimpleName(),
        StringUtils.join(commandLine, " ")));
    for (String key : keys) {
      sLogger.info(String.format(
          "%s: Parameter: %s=%s", this.getClass().getSimpleName(), key,
          stage_options.get(key).toString()));
    }
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
