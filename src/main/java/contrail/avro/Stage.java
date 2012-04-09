package contrail.avro;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import contrail.Contrail;

/** 
 * An abstract base class for each stage of processing. 
 *
 */
public abstract class Stage extends Configured implements Tool  {
  private static final Logger sLogger = 
      Logger.getLogger(Stage.class);
  public Stage() {
    initialize();
  }
  
  protected void initialize(){
    initializeDefaultOptions();
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
   * Return a list of command line options used by this stage.
   * 
   */
  protected List<Option> getCommandLineOptions() {
    List<Option> options = new ArrayList<Option>();

    // Return options used by all stages.
    options.addAll(ContrailOptions.getHelpOptions());
    options.addAll(ContrailOptions.getHadoopOptions());
    options.addAll(ContrailOptions.getStageOptions());
    return options;
  }

  /**
   * Process the command line options.
   * 
   * TODO(jlewi): How should we inform the user of missing arguments?
   */
  protected void parseCommandLine(String[] args) {
    Options options = new Options();

    for (Iterator<Option> it = getCommandLineOptions().iterator();
        it.hasNext();) {
      options.addOption(it.next());
    }
    CommandLineParser parser = new GnuParser();
    CommandLine line;
    try 
    {
      line = parser.parse(options, args );
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
    if (line.hasOption("help") || line.hasOption("h")) {
      printHelp();
      // TODO(jlewi): We need to refactor the code below. The help strings
      // for the individual options should come from the options themselves.
      //    We should use the HelpFormatter  class to print out the help information.

      //      System.out.print("Contrail version " + Contrail.VERSION + " (Schatz et al. http://contrail-bio.sf.net)\n" +
      //          "\n" +
      //          "Usage: Contrail [-asm dir] [-reads dir] [-k k] [options]\n" +
      //          "\n" +
      //          "Contrail Stages\n" +
      //          "================\n" +
      //          "preprocess : convert from fastq format\n" +
      //          "  -filesuffix         : use filename suffix (_1 or _2) as readname suffix\n" +
      //          "\n" +
      //          "buildInitial : build initial de Bruijn graph\n" +
      //          "  -k <bp>             : Initial graph node length\n" +
      //          "  -maxthreads <max>   : Max reads to thread a node [" + MAXTHREADREADS + "]\n" +
      //          "  -maxr5 <max>        : Max number of reads starting at a node [" + MAXR5 +"]\n" +
      //          "  -trim5 <t5>         : Bases to chop on 5' [" + TRIM5 + "]\n" +
      //          "  -trim3 <t3>         : Bases to chop on 3' [" + TRIM3 + "]\n" +
      //          "\n" +
      //          "removeTips : remove deadend tips\n" +
      //          "  -tiplen <len>       : Max tip length [" + -TIPLENGTH + "K]\n" +
      //          "\n" +
      //          "popBubbles : pop bubbles\n" +
      //          "  -bubblelen <len>    : Max Bubble length [" + -MAXBUBBLELEN + "K]\n" +
      //          "  -bubbleerrate <len> : Max Bubble Error Rate [" + BUBBLEEDITRATE + "]\n" +
      //          "\n" +
      //          "lowcov : cut low coverage nodes\n" +
      //          "  -lowcov <thresh>    : Cut coverage threshold [" + LOW_COV_THRESH + "]\n" +
      //          "  -lowcovlen <len>    : Cut length threshold [" + -MAX_LOW_COV_LEN + "K]\n" +
      //          "\n" +
      //          "repeats : thread short repeats\n" +
      //          "  -threads <min>      : Number threading reads [" + MIN_THREAD_WEIGHT + "]\n" +
      //          "  -maxthreads <max>   : Max reads to thread a node [" + MAXTHREADREADS + "]\n" +
      //          "  -record_all_threads : Record threads on non-branching nodes\n" +
      //          "\n" +
      //          "scaffolding : scaffold together contigs using mates\n" +
      //          "  -insertlen <len>    : Expected Insert size [required]\n" +
      //          "  -minuniquecov <min> : Min coverage to scaffold [required]\n" +
      //          "  -maxuniquecov <max> : Max coverage to scaffold [required]\n" +
      //          "  -minuniquelen <min> : Min contig len to scaffold [" + -MIN_CTG_LEN + "K]\n" +
      //          "  -maxfrontier <hops> : Max hops to explore [" + MAX_FRONTIER + "]\n" +
      //          "  -wiggle <bp>        : Insert wiggle length [" + MIN_WIGGLE + "]\n" +
      //          "\n" +
      //          "convertFasta : convert final assembly to fasta format\n" +
      //          "  -genome <len>       : Genome size for N50 computation\n" +
      //          "\n" +
      //          "General Options\n" +
      //          "===============\n" +
      //          "  -asm <asmdir>       : Hadoop Base directory for assembly [required]\n" +
      //          "  -reads <readsdir>   : Directory with reads [required]\n" + 
      //          "  -work <workdir>     : Local directory for output files [" + localBasePath + "]\n" +
      //          "  -slots <slots>      : Hadoop Slots to use [" + HADOOP_MAPPERS + "]\n" +
      //          "  -expert             : Show expert options\n");
      //
      //
      //      if (line.hasOption("expert")) {
      //        System.out.print("\n" +
      //            "Conversion options\n" +
      //            "================\n" +
      //            "  -convert_fa <dir>      : Just convert assembly <dir> to fasta\n" +
      //            "  -run_stats <dir>       : Just compute size stats of <dir>\n" +
      //            "    -genome <len>        : Genome size for N50 computation\n" +
      //            "  -print_fa <file>       : Convert localfile <file> to fasta\n" +
      //            "    -fasta_min_len <len> : Just convert contigs with this len [" + FASTA_MIN_LEN + "]\n"+
      //            "    -fasta_min_cov <cov> : Just convert contigs with this cov [" + FASTA_MIN_COV + "]\n"+
      //            "\n" +
      //            "Hadoop Options\n" +
      //            "==============\n" +
      //            "  -nodes <max>        : Max nodes in memory [" + HADOOP_LOCALNODES + "]\n" +
      //            "  -javaopts <opts>    : Hadoop Java Opts [" + HADOOP_JAVAOPTS + "]\n" +
      //            "  -timeout <usec>     : Hadoop task timeout [" + HADOOP_TIMEOUT + "]\n" +
      //            "  -validate           : Just validate options\n" +
      //            "  -go                 : Execute even when validating\n" +
      //            "\n" +
      //            "Assembly Restart Options\n" +
      //            "========================\n" +
      //            "You must specific a start stage if you do not want to start at the very beginning\n" +
      //            "It is also recommended that you restart using a new assembly directory:\n" +
      //            "$ hadoop fs -cp /path/to/assembly/10-* /path/to/newassembly/\n" +
      //            "$ hadoop jar contrail.jar -asm /path/to/newassembly -start scafolding <...>\n\n" +
      //            "  -start <stage>      : Start stage: buildInitial|removeTips|popBubbles|lowcov|repeats|scaffolding|convertFasta\n" +
      //            "  -stop  <stage>      : Stop stage (default: convertFasta)\n" +
      //            "  -restart_initial               : Restart after build initial, before quickmerge\n" +
      //            "  -restart_compress <stage>      : Restart compress after this completed stage\n" +
      //            "  -restart_compress_remain <cnt> : Restart compress with these remaining\n" +
      //            "  -restart_tip <stage>           : Restart tips after this completed tips\n" +
      //            "  -restart_tip_remain <cnt>      : Restart tips with these remaining\n" +
      //            "  -restart_scaff_phase <phase>   : Restart at this phase\n" +
      //            "  -restart_scaff_stage <stage>   : Restart at this stage: edges, bundles, frontier, update, finalize, clean\n" + 
      //            "  -restart_scaff_frontier <hops> : Restart after this many hops\n"
      //            );
      System.exit(0);
    } // has help

    if (line.hasOption("foroozie")) {
      stage_options.put("foroozie", line.getOptionValue("foroozie"));
    }
    
    if (line.hasOption("writeconfig")) {
      stage_options.put("writeconfig", line.getOptionValue("writeconfig"));
    }
    
  } 

  // TODO(jlewi): Refactor code below to parse out common options. 
  //        if (line.hasOption("validate")) { validateonly = true; }
  //        if (line.hasOption("go"))       { forcego = true; }
  //        
  //        if (line.hasOption("asm"))   { hadoopBasePath = line.getOptionValue("asm");  }
  //        if (line.hasOption("reads")) { hadoopReadPath = line.getOptionValue("reads"); }
  //        if (line.hasOption("work"))  { localBasePath  = line.getOptionValue("work"); }
  //        
  //        if (line.hasOption("slots"))    { HADOOP_MAPPERS  = Integer.parseInt(line.getOptionValue("slots")); HADOOP_REDUCERS = HADOOP_MAPPERS; }
  //        if (line.hasOption("nodes"))    { HADOOP_LOCALNODES      = Integer.parseInt(line.getOptionValue("nodes")); }
  //        if (line.hasOption("javaopts")) { HADOOP_JAVAOPTS = line.getOptionValue("javaopts"); }
  //        if (line.hasOption("timeout"))  { HADOOP_TIMEOUT  = Long.parseLong(line.getOptionValue("timeout")); }
  //        
  //        if (line.hasOption("start")) { STARTSTAGE = line.getOptionValue("start"); }
  //        if (line.hasOption("stop"))  { STOPSTAGE  = line.getOptionValue("stop");  }
  //        
  //        if (line.hasOption("restart_initial"))    { RESTART_USED = true; RESTART_INITIAL    = 1; }
  //        if (line.hasOption("restart_tip"))        { RESTART_USED = true; RESTART_TIP        = Integer.parseInt(line.getOptionValue("restart_tip")); }
  //        if (line.hasOption("restart_tip_remain")) { RESTART_USED = true; RESTART_TIP_REMAIN = Integer.parseInt(line.getOptionValue("restart_tip_remain")); }
  //
  //        if (line.hasOption("restart_compress"))        { RESTART_USED = true; RESTART_COMPRESS        = Integer.parseInt(line.getOptionValue("restart_compress")); }
  //        if (line.hasOption("restart_compress_remain")) { RESTART_USED = true; RESTART_COMPRESS_REMAIN = Integer.parseInt(line.getOptionValue("restart_compress_remain")); }
  //        
  //        if (line.hasOption("restart_scaff_phase"))    { RESTART_USED = true; RESTART_SCAFF_PHASE    = Integer.parseInt(line.getOptionValue("restart_scaff_phase")); }
  //        if (line.hasOption("restart_scaff_stage"))    { RESTART_USED = true; RESTART_SCAFF_STAGE    = line.getOptionValue("restart_scaff_stage"); }
  //        if (line.hasOption("restart_scaff_frontier")) { RESTART_USED = true; RESTART_SCAFF_FRONTIER = Integer.parseInt(line.getOptionValue("restart_scaff_frontier")); }
  //        
  //        if (line.hasOption("filesuffix"))    { PREPROCESS_SUFFIX = 1; }
  //        
  //        if (line.hasOption("k"))     { K     = Long.parseLong(line.getOptionValue("k")); }
  //        if (line.hasOption("maxr5")) { MAXR5 = Long.parseLong(line.getOptionValue("maxr5")); }
  //        if (line.hasOption("trim3")) { TRIM3 = Long.parseLong(line.getOptionValue("trim3")); }
  //        if (line.hasOption("trim5")) { TRIM5 = Long.parseLong(line.getOptionValue("trim5")); }
  //        
  //        if (line.hasOption("tiplen"))       { TIPLENGTH      = Long.parseLong(line.getOptionValue("tiplen")); }
  //        if (line.hasOption("bubblelen"))    { MAXBUBBLELEN   = Long.parseLong(line.getOptionValue("bubblelen")); }
  //        if (line.hasOption("bubbleerate"))  { BUBBLEEDITRATE = Long.parseLong(line.getOptionValue("bubbleerate")); }
  //        
  //        if (line.hasOption("lowcov"))       { LOW_COV_THRESH    = Float.parseFloat(line.getOptionValue("lowcov")); }
  //        if (line.hasOption("lowcovlen"))    { MAX_LOW_COV_LEN   = Long.parseLong(line.getOptionValue("lowcovlen")); }
  //
  //        if (line.hasOption("threads"))             { MIN_THREAD_WEIGHT = Long.parseLong(line.getOptionValue("threads")); }
  //        if (line.hasOption("maxthreads"))          { MAXTHREADREADS    = Long.parseLong(line.getOptionValue("maxthreads")); }
  //        if (line.hasOption("record_all_threads"))  { RECORD_ALL_THREADS = 1; }
  //        
  //        if (line.hasOption("insertlen"))    { INSERT_LEN     = Long.parseLong(line.getOptionValue("insertlen")); }
  //        if (line.hasOption("wiggle"))       { MIN_WIGGLE     = Long.parseLong(line.getOptionValue("wiggle")); }
  //        if (line.hasOption("minuniquelen")) { MIN_CTG_LEN    = Long.parseLong(line.getOptionValue("minuniquelen")); }
  //        if (line.hasOption("minuniquecov")) { MIN_UNIQUE_COV = Float.parseFloat(line.getOptionValue("minuniquecov")); }
  //        if (line.hasOption("maxuniquecov")) { MAX_UNIQUE_COV = Float.parseFloat(line.getOptionValue("maxuniquecov")); }
  //        if (line.hasOption("maxfrontier"))  { MAX_FRONTIER = Long.parseLong(line.getOptionValue("maxfrontier")); }
  //    
  //        if (line.hasOption("genome"))       { N50_TARGET = Long.parseLong(line.getOptionValue("genome")); }
  //        if (line.hasOption("run_stats"))    { RUN_STATS = line.getOptionValue("run_stats"); }
  //        
  //        if (line.hasOption("convert_fa"))    { CONVERT_FA    = line.getOptionValue("convert_fa"); }
  //        if (line.hasOption("print_fa"))      { PRINT_FA      = line.getOptionValue("print_fa"); }
  //        if (line.hasOption("fasta_min_len")) { FASTA_MIN_LEN = Long.parseLong(line.getOptionValue("fasta_min_len")); }
  //        if (line.hasOption("fasta_min_cov")) { FASTA_MIN_COV = Float.parseFloat(line.getOptionValue("fasta_min_cov")); }

  /**
   * Initialize the hadoop job configuration with information needed for this
   * stage.
   * 
   * @param conf: The job configuration.
   */
  protected void initializeJobConfiguration(JobConf conf) {
    // Any option which isn't specified explictly should be added
    HashMap<String, Object> defaults = default_options;
    for (Iterator<String> key_it = defaults.keySet().iterator();
         key_it.hasNext();) {
      String key = key_it.next();
      if (!stage_options.containsKey(key)) {
        stage_options.put(key, defaults.get(key));
      }
    }
    
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
      Object value = stage_options.get(key);
      if (value instanceof  String) {
        conf.set(key, (String)value);
      } else if (value instanceof Long){
        conf.setLong(key, (Long)(value));
      } else {
        throw new RuntimeException("option: " + key + " doesn't have an " + 
            "acceptable type");
      }
    }
  }
  
  protected HashMap<String, Object> default_options;
  
  /**
   * Initialize the mapping containing default values for the options.
   */
  protected void initializeDefaultOptions() {    
     default_options = new HashMap<String, Object>();   
  }
  
  /**
   * Print the help message.
   */
  protected void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    Options options = new Options();
    for (Option option: getCommandLineOptions()) {
      options.addOption(option);
    }
    formatter.printHelp(
        "hadoop jar CONTRAILJAR MAINCLASS [options]", options);
  }
  /**
   * Run the stage.
   */
  protected int run(Map<String, Object> options) throws Exception  {
    // Copy the options from the input.
    stage_options.putAll(options);
    
    // Run the stage.
    return run();
  }
    
  public int run(String[] args) throws Exception {
    sLogger.info("Tool name: " + this.getClass().getName());
    parseCommandLine(args);   
    return run();
  }
  /**
   * Run the stage.
   */
  abstract protected int run() throws Exception;
  
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
      try {
        // Oozie requires certain properties to be specified in the workflow
        // and not in the individual job configuration stages.
        HashSet<String> exclude = new HashSet<String>();
        exclude.add("fs.default.name");
        exclude.add("mapred.job.tracker");
        
        File xml_file = new File(jobpath.toString());
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
            jobpath.toString());
        sLogger.error("Exception:" + exception.toString());
      }
    }
    
    sLogger.info("Wrote job config to:" + jobpath.toString());
  }
}

