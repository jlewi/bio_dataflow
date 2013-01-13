package contrail.pipelines;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import contrail.correct.*;
import contrail.correct.BuildBitVector;
import contrail.correct.ConvertKMerCountsToText;
import contrail.correct.CutOffCalculation;
import contrail.correct.InvokeFlash;
import contrail.correct.InvokeQuake;
import contrail.correct.KmerCounter;
import contrail.stages.ContrailParameters;
import contrail.stages.NotImplementedException;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

public class CorrectionPipelineRunner extends Stage{
  private static final Logger sLogger = Logger.getLogger(CorrectionPipelineRunner.class);

  public CorrectionPipelineRunner() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }

  protected void setDefaultParameters() {
    // This function is intended to be overloaded in subclasses which
    // customize the parameters for different datasets.
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
    Stage[] substages =
      {
        new RekeyReads(), new JoinReads(), new InvokeFlash(), new KmerCounter(), new ConvertKMerCountsToText(), new CutOffCalculation(),
        new BuildBitVector(), new InvokeQuake() 
      };

    for (Stage stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }

    //The outputpath is a name of a directory in which all the outputs are placed
    ParameterDefinition flashInputPath = new ParameterDefinition(
        "flash_input", "The inputpath of mate pairs on which flash is to be run", String.class, new String(""));
    ParameterDefinition quakeNonMateInputPath = new ParameterDefinition(
        "quake_input_non_mate", "The inputpath of non mate pairs on which" +
            "quake is to be run", String.class, new String(""));
    ParameterDefinition quakeMateInputPath = new ParameterDefinition(
        "quake_input_mate", "The inputpath of matepairs on which quake is to be run", String.class, new String(""));

    for (ParameterDefinition def: new ParameterDefinition[] {flashInputPath, quakeNonMateInputPath,quakeMateInputPath }) {
      definitions.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * This method runs the entire pipeline
   * @throws Exception
   */
  private void runCorrectionPipeline() throws Exception{


    //TODO(dnettem) - Make this more generic. Currently this is quick and dirty, since
    // we know for sure that Flash will run.


    String[] required_args = {"flash_input", "flash_binary","flash_notcombined","splitSize"};

    String rawFlashInput = (String) stage_options.get("flash_input");
    String flashBinary = (String) stage_options.get("flash_binary");
    String notCombinedFlash = (String) stage_options.get("flash_notcombined");

    String outputDirectory = (String) stage_options.get("outputpath");

    String quakeNonMateInput = (String) stage_options.get("quake_input_non_mate");
    String quakeMateInput = (String) stage_options.get("quake_input_mate");
    String quakeBinary = (String) stage_options.get("quake_binary");

    String rekeyOutputPath = "";
    String flashOutputPath = "";
    String joinedFlashInput = "";
    String notCombinedAvro = "";
    String joinedNotCombined = "";
    String kmerCounterOutputPath = "";
    String nonAvroKmerCountOutput = "";
    String bitVectorDirectory = "";
    String bitVectorPath = "";
    String stageInput = "";

    int cutoff = 0;
    boolean flashInvoked = false;
    Stage stage;

    // TODO(dnettem): Initial rekey stage for singles data
    // Our current GAGE Dataset does not have these kind of reads.

    // Rekey Flash Mate Pairs

    if(rawFlashInput.trim().length() != 0){
      sLogger.info("Rekeying Flash Input Data");
      stage = new RekeyReads();
      rekeyOutputPath = runStage(stage, rawFlashInput, outputDirectory, "");
    }

    else{
      sLogger.info("Skipping Flash execution");
    }

    // Join for Flash.
    sLogger.info("Joining Rekeyed Reads for Flash");
    stage = new JoinReads();
    joinedFlashInput = runStage(stage, rekeyOutputPath, outputDirectory, "");

    // Invoke Flash
    if(joinedFlashInput.trim().length() != 0 && flashBinary.trim().length() != 0){
      sLogger.info("Running Flash");
      stage = new InvokeFlash();
      flashOutputPath = runStage(stage, joinedFlashInput, outputDirectory, "");
      flashInvoked = true;
    }

    else{
      sLogger.info("Skipping Flash execution");
    }

    if((stageInput.trim().length() != 0 || quakeNonMateInput.trim().length() != 0 || quakeMateInput.trim().length()!=0) 
        && quakeBinary.trim().length()!=0 ) {

      // Kmer counting stage
      sLogger.info("Running KmerCounter");
      stage = new KmerCounter();

      stageInput = stageInput + quakeNonMateInput + "," + quakeMateInput;

      // We add the flash output directory only if the user has run flash
      if(flashInvoked){
        stageInput = stageInput + "," + flashOutputPath;
      }
      kmerCounterOutputPath = runStage(stage, stageInput, outputDirectory, "");

      //Convert KmerCounts to Non Avro stage
      sLogger.info("Running ConvertKMerCountsToText");
      stage = new ConvertKMerCountsToText();
      stageInput = new Path(kmerCounterOutputPath, "part-00000.avro").toString();
      nonAvroKmerCountOutput = runStage(stage, stageInput, outputDirectory, "");

      // Cutoff calculation stage 
      sLogger.info("Running CutOffCalculation");
      CutOffCalculation cutoffObj = new CutOffCalculation();
      stageInput = new Path(nonAvroKmerCountOutput, "part-00000").toString();
      runStage(cutoffObj, stageInput, outputDirectory,  "");

      //Bitvector construction 
      sLogger.info("Running BuildBitVector");
      cutoff = cutoffObj.getCutoff();
      stage_options.put("cutoff", cutoff);
      stage = new BuildBitVector();
      stageInput = kmerCounterOutputPath;
      bitVectorDirectory = runStage(stage, stageInput, outputDirectory, "");
      bitVectorPath = new Path(bitVectorDirectory, "bitvector").toString();
      stage_options.put("bitvectorpath", bitVectorPath);

      //Invoke Quake on Non Mate Pairs 
      // TODO(dnettem) - for now all quake inputs dumped together.
      if(quakeNonMateInput.trim().length() !=0 ){
        sLogger.info("Running Quake for Non Mate Pairs");
        stage = new InvokeQuake();
        stageInput = quakeNonMateInput;
        if(flashInvoked){
          stageInput = stageInput + "," +  flashOutputPath + "," + quakeMateInput;
        }
        runStage(stage, stageInput, outputDirectory, "non_mate_");
      }
      else{
        sLogger.info("Skipping running Quake for non mate pairs");
      }

      /*
      // Invoke quake for Mate Pairs
      if(quakeMateInput.trim().length() !=0 ){
        sLogger.info("Running Quake for Mate Pairs");
        stage = new InvokeQuake();
        stageInput = quakeMateInput;
        if (flashInvoked)
          stageInput = stageInput + "," + joinedNotCombined;
        runStage(stage, stageInput, outputDirectory, "mate_");
      }
      else{
        sLogger.info("Skipping running Quake for mate pairs");
      }
      */

    }
    else{
      sLogger.info("Skipping Quake execution");
    }

  }

  /**
   * Runs a particular stage and returns the path containing the output
   * The output path is OutputDirectory/prefix+className
   * @param stage
   * @param inputPath
   * @param flashBinary
   * @param outputDirectory
   * @return
   * @throws Exception
   */
  private String runStage(Stage stage, String inputPath, String outputDirectory, String prefix) throws Exception{

    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            stage.getParameterDefinitions().values());
    stage.setConf(getConf());
    stageOptions.put("inputpath", inputPath);
    String outputPath = new Path(outputDirectory, (prefix + stage.getClass().getName()) ).toString(); 
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);
    RunningJob job = stage.runJob();
    if (job !=null && !job.isSuccessful()) {
      throw new RuntimeException(
          String.format(
              "Stage %s had a problem", stage.getClass().getName()));
    }
    return outputPath;
  }

  public RunningJob runJob() throws Exception {
    if (stage_options.containsKey("writeconfig")) {
      throw new NotImplementedException(
          "Support for writeconfig isn't implemented yet for " +
          "AssembleContigs");
    } else {
      long starttime = System.currentTimeMillis();
      runCorrectionPipeline();
      long endtime = System.currentTimeMillis();

      float diff = (float) ((endtime - starttime) / 1000.0);
      sLogger.info("Runtime: " + diff + " s");
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CorrectionPipelineRunner(), args);
    System.exit(res);
  }
}
