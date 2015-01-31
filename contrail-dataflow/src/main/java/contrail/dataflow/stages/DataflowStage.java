package contrail.dataflow.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import contrail.dataflow.DataflowParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * Stage with hooks for customizing different parts of execution for a stage
 * which runs a Dataflow pipeline.
 *
 */
public abstract class DataflowStage extends NonMRStage{
  private static final Logger sLogger = Logger.getLogger(
      DataflowStage.class);

  protected PipelineOptions pipelineOptions;

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
      HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def: DataflowParameters.getDefinitions()) {
      defs.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(defs);
  }

  /**
   * Setup the writer for stage information.
   */
  @Override
  protected void setupInfoWriter() {
    // TODO(jeremy@lewi.us): The implementation of stage info writer assumes
    // we can use the hadoop filesystem. If we're using Dataflow we don't
    // want to require a hadoop configuration.
    // For now we do nothing.
    sLogger.warn("Skipping setup of stage info writer.");
  }

  // TODO(jlewi): Should we return a factory rather than the options
  // themselves?
  protected PipelineOptions buildPipelineOptions() {

    PipelineOptions options = PipelineOptionsFactory.create();
    DataflowParameters.setPipelineOptions(stage_options, options);
    return options;
  }

  protected abstract Pipeline buildPipeline(PipelineOptions options);

  @Override
  protected void stageMain() {
    // TODO Auto-generated method stub
    this.pipelineOptions = buildPipelineOptions();

    Pipeline pipeline = buildPipeline(this.pipelineOptions);
    pipeline.run();
  }
}
