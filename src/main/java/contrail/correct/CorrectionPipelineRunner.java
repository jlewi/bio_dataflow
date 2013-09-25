package contrail.correct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.PipelineStage;
import contrail.stages.StageBase;
import contrail.util.ContrailLogger;


// TODO(jeremy@lewi.us): Why is bitvectorpath listed as an argument
// when you do "--help=true", shouldn't it get set automatically? It is
// used by BuildBitVector stage. We should exclude that option from
// the options for the CorrectionPipelineRunner stage because it should be
// derived automatically from the other parameters.
//
// TODO(jeremy@lewi.us): How should the user indicate that they don't
// want to run flash? One option would be to allow the empty string for
// flash_input. However, that makes it difficult to distinguish the user
// forgot to set the path or if they don't want to use flash. I think a
// better option is to add a boolean option to disable flash.
//
// TODO(jeremy@lewi.us): The code currently assumes that the quake paths
// and the flash paths are disjoint. Otherwise we would input some reads
// to quake twice and that would be bad. We should add a check to verify this
// and fail if this assumption is violated.
//
// TODO(jeremy@lewi.us): Quake has a mode which supports mate pairs; which
// I think just has to do with treating both pairs of a read the "same".
// Currently we don't use that mode and treat all reads as non-mate pairs
// for the purpose of quake.
public class CorrectionPipelineRunner extends PipelineStage {
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      CorrectionPipelineRunner.class);

  public CorrectionPipelineRunner() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }

  @Override
  protected void setDefaultParameters() {
    // This function is intended to be overloaded in subclasses which
    // customize the parameters for different datasets.
    super.setDefaultParameters();
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();
    definitions.putAll(super.createParameterDefinitions());
    // We add all the options for the stages we depend on.
    StageBase[] substages =
      {new JoinReads(), new InvokeFlash(), new KmerCounter(),
       new ConvertKMerCountsToText(), new CutOffCalculation(),
       new BuildBitVector(), new InvokeQuake()};

    for (StageBase stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    // Remove parameters inherited from the substages which get set
    // automatically or are derived from other parameters.
    definitions.remove("cutoff");
    definitions.remove("bitvectorpath");
    definitions.remove("inputpath");

    //The outputpath is a name of a directory in which all the outputs are placed
    ParameterDefinition flashInputPath = new ParameterDefinition(
        "flash_input", "The path to the fastq files to run flash on. This " +
        "can be a glob expression.",
        String.class, null);

    ParameterDefinition quakeNonMateInputPath = new ParameterDefinition(
        "no_flash_input",
        "The path to the fastq files which should be included in " +
        "quake but which we don't run flash on. This can be a glob expression.",
        String.class, null);

    definitions.put(flashInputPath.getName(), flashInputPath);
    definitions.put(quakeNonMateInputPath.getName(), quakeNonMateInputPath);

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * Join the FastQRecords corresponding to mate pairs.
   *
   * @param flashInputAvroPath
   * @return
   */
  private void runJoinMatePairs(String inputPath, String outputPath) {
    sLogger.info("Join mate pairs.");
    JoinReads stage = new JoinReads();
    stage.initializeAsChild(this);

    HashMap<String, Object> parameters =new HashMap<String, Object>();
    parameters.put("inputpath", inputPath);
    parameters.put("outputpath", outputPath);
    stage.setParameters(parameters);

    if (!executeChild(stage)) {
      sLogger.fatal(String.format(
          "Stage %s had a problem", stage.getClass().getName()),
          new RuntimeException("Stage failure."));
    }
  }

  /**
   *
   * @param flashResults: The results of running flash.
   */
  private void runQuake(FlashResults flashResults) {
    String outputPath = FilenameUtils.concat(
        (String) stage_options.get("outputpath"), "quake");

    // Convert the fastq files which we want to use for quake but not flash.
    String inputAvroPath = FilenameUtils.concat(
        outputPath, FastQToAvro.class.getSimpleName());

    {
      FastQToAvro inputToAvro = new FastQToAvro();
      HashMap<String, Object> options = new HashMap<String, Object>();
      options.put("inputpath", stage_options.get("no_flash_input"));
      options.put("outputpath", inputAvroPath);
      inputToAvro.setParameters(options);
      if (!inputToAvro.execute()) {
        sLogger.fatal(
            String.format(
                "Stage %s had a problem", inputToAvro.getClass().getName()),
            new RuntimeException("Stage failed"));
      }
    }

    ArrayList<String> inputGlobs = new ArrayList<String>();
    inputGlobs.add(flashResults.flashOutputPath);
    inputGlobs.add(inputAvroPath);

    // Kmer counting stage
    sLogger.info("Running KmerCounter");
    KmerCounter kmerCounter = new KmerCounter();

    String kmerCountsPath = FilenameUtils.concat(
        outputPath, kmerCounter.getClass().getSimpleName());
    String kmerInputPaths = StringUtils.join(inputGlobs, ",");
    Map<String, Object> counterOptions = ContrailParameters.extractParameters(
            stage_options, kmerCounter.getParameterDefinitions().values());
    counterOptions.put("inputpath", kmerInputPaths);
    counterOptions.put("outputpath", kmerCountsPath);
    counterOptions.put("K", stage_options.get("K"));
    kmerCounter.setParameters(counterOptions);

    // TODO(jeremy@lewi.us): We should be using executeChild
    // once CorrectionPipelineRunner is converted to a subclass of
    // PipelineStage
    if (!kmerCounter.execute()) {
      sLogger.fatal(
          String.format(
              "Stage %s had a problem", kmerCounter.getClass().getName()),
          new RuntimeException("Stage failed"));
    }

    // Convert KmerCounts to Text
    sLogger.info("Running ConvertKMerCountsToText");
    ConvertKMerCountsToText converter = new ConvertKMerCountsToText();
    converter.initializeAsChild(this);
    HashMap<String, Object> convertOptions = new HashMap<String, Object>();

    String textCountsPath = FilenameUtils.concat(
        outputPath, converter.getClass().getSimpleName());
    convertOptions.put("inputpath", kmerCountsPath);
    convertOptions.put("outputpath", textCountsPath);
    converter.setParameters(convertOptions);
    if (!executeChild(converter)) {
      sLogger.fatal(String.format(
          "Stage %s had a problem", converter.getClass().getName()),
          new RuntimeException("Stage failure."));
      System.exit(-1);
    }

    // Cutoff calculation stage
    sLogger.info("Running CutOffCalculation");
    CutOffCalculation cutoffStage = new CutOffCalculation();
    cutoffStage.initializeAsChild(this);

    Map<String, Object> cutoffOptions = ContrailParameters.extractParameters(
        stage_options, cutoffStage.getParameterDefinitions().values());

    String cutoffPath = FilenameUtils.concat(
        outputPath, cutoffStage.getClass().getSimpleName());
    cutoffOptions.put("inputpath", textCountsPath);
    cutoffOptions.put("outputpath", cutoffPath);
    cutoffStage.setParameters(cutoffOptions);
    if (!executeChild(cutoffStage)) {
      sLogger.fatal(String.format(
          "Stage %s had a problem", cutoffStage.getClass().getName()),
          new RuntimeException("Stage failure."));
      System.exit(-1);
    }

    // Bitvector construction
    sLogger.info("Running BuildBitVector");
    int cutoff = cutoffStage.getCutoff();
    BuildBitVector bitVectorStage = new BuildBitVector();
    bitVectorStage.initializeAsChild(this);

    String bitVectorPath = FilenameUtils.concat(
        outputPath, bitVectorStage.getClass().getSimpleName());

    Map<String, Object> vectorOptions = ContrailParameters.extractParameters(
        stage_options, bitVectorStage.getParameterDefinitions().values());
    vectorOptions.put("inputpath", kmerCountsPath);
    vectorOptions.put("outputpath", bitVectorPath);
    vectorOptions.put("cutoff", cutoff);
    bitVectorStage.setParameters(vectorOptions);
    if (!executeChild(bitVectorStage)) {
      sLogger.fatal(
          "Stage: " +  bitVectorStage.getClass().getSimpleName() + " failed.",
          new RuntimeException("Stage failure."));
      System.exit(-1);
    }

    sLogger.info("Running Quake.");
    InvokeQuake quakeStage = new InvokeQuake();
    quakeStage.initializeAsChild(this);
    Map<String, Object> quakeOptions = ContrailParameters.extractParameters(
        stage_options, quakeStage.getParameterDefinitions().values());
    quakeOptions.put("inputpath", StringUtils.join(inputGlobs, ","));

    String quakePath = FilenameUtils.concat(
        outputPath, quakeStage.getClass().getSimpleName());
    quakeOptions.put("outputpath", quakePath);
    quakeOptions.put(
        "bitvectorpath", bitVectorStage.getBitVectorPath().toString());
    quakeStage.setParameters(quakeOptions);

    if (!executeChild(quakeStage)) {
      sLogger.fatal(
          "Stage: " +  quakeStage.getClass().getSimpleName() + " failed.",
          new RuntimeException("Stage failure."));
      System.exit(-1);
    }
  }

  private class FlashResults {
    public String flashOutputPath;
  }

  private FlashResults runFlash() {
    String outputPath = FilenameUtils.concat(
        (String) stage_options.get("outputpath"), "flash");
    // The output will be organized into subdirectories for flash and quake
    // respectively.
    String flashOutputPath = FilenameUtils.concat(outputPath, "flash");

    // Convert the fastq files to run flash on to to avro files.
    String flashInputAvroPath = FilenameUtils.concat(
        flashOutputPath, FastQToAvro.class.getSimpleName());

    {
      FastQToAvro flashInputToAvro = new FastQToAvro();
      HashMap<String, Object> options = new HashMap<String, Object>();
      options.put("inputpath", stage_options.get("flash_input"));
      options.put("outputpath", flashInputAvroPath);
      flashInputToAvro.setParameters(options);
      if (!flashInputToAvro.execute()) {
        sLogger.fatal(
            String.format(
                "Stage %s had a problem",
                flashInputToAvro.getClass().getName()),
            new RuntimeException("Stage failed"));
      }
    }

    // Join mate pairs.
    String flashJoinedPath = FilenameUtils.concat(
        flashOutputPath, JoinReads.class.getSimpleName());
    runJoinMatePairs(flashInputAvroPath, flashJoinedPath);

    // Invoke Flash
    // TODO(jeremy@lewi.us): We should add a boolean option to skip flash.
    // or else create a subpipeline which just runs quake.
    String flashOutput = FilenameUtils.concat(
        flashOutputPath, InvokeFlash.class.getSimpleName());
    {
      sLogger.info("Running Flash");
      InvokeFlash invokeFlash = new InvokeFlash();
      invokeFlash.initializeAsChild(this);

      Map<String, Object> parameters =
          ContrailParameters.extractParameters(
              stage_options, invokeFlash.getParameterDefinitions().values());
      parameters.put("inputpath", flashJoinedPath);
      parameters.put("outputpath", flashOutput);
      invokeFlash.setParameters(parameters);

      if (!executeChild(invokeFlash)) {
        sLogger.fatal(String.format(
            "Stage %s had a problem", invokeFlash.getClass().getName()),
            new RuntimeException("Stage failure."));
      }
    }

    FlashResults results = new FlashResults();
    results.flashOutputPath = flashOutput;
    return results;
  }

  /**
   * This method runs the entire pipeline
   * @throws Exception
   */
  private void runCorrectionPipeline() {
    FlashResults flashResults = runFlash();
    runQuake(flashResults);
  }

  @Override
  public void stageMain() {
    try {
      runCorrectionPipeline();
    } catch (Exception e) {
      sLogger.fatal("AssembleContigs failed.", e);
      System.exit(-1);
    }
  }
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CorrectionPipelineRunner(), args);
    System.exit(res);
  }
}
