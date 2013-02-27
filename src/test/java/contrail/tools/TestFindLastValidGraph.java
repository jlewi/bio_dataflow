package contrail.tools;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;
import org.junit.Test;

import static org.junit.Assert.*;
import contrail.graph.GraphNode;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.stages.BuildGraphAvro;
import contrail.stages.CompressAndCorrect;
import contrail.stages.CounterInfo;
import contrail.stages.QuickMergeAvro;
import contrail.stages.RemoveTipsAvro;
import contrail.stages.StageInfo;
import contrail.stages.StageParameter;
import contrail.stages.StageState;
import contrail.util.FileHelper;

// TODO(jlewi): Better testing.
// We should test the case where we have a pipeline that moves the output
// produced by a stage. In this case the true outputpath should be stored
// in the modified_parameters field of StageInfo.
public class TestFindLastValidGraph {

  /**
   * Create a stage to use for testing.
   *
   * @param outputPath
   * @param nodes
   * @param className
   * @return
   */
  private StageInfo createStage(
      String outputPath, Collection<GraphNode> nodes, String className) {
    StageInfo stageInfo = new StageInfo();
    stageInfo.setStageClass(className);
    stageInfo.setState(StageState.SUCCESS);
    stageInfo.setCounters(new ArrayList<CounterInfo>());
    stageInfo.setParameters(new ArrayList<StageParameter>());
    stageInfo.setModifiedParameters(new ArrayList<StageParameter>());
    stageInfo.setSubStages(new ArrayList<StageInfo>());

    File outputPathFile = new File(outputPath);
    if (!outputPathFile.exists()) {
      outputPathFile.mkdirs();
    }

    File avroFile = new File(FilenameUtils.concat(outputPath, "graph.avro"));
    GraphUtil.writeGraphToFile(avroFile, nodes);

    StageParameter outputParameter = new StageParameter();
    outputParameter.setName("outputpath");
    outputParameter.setValue(outputPath);
    stageInfo.getParameters().add(outputParameter);
    return stageInfo;
  }

  private static class TestCase {
    String jsonFile;
    StageInfo errorStage;
  }

  // Create all the input data. Returns the path to the json file
  // containing the StageInfo for the pipeline.
  private TestCase createPipeline(String testDir) {
    TestCase testCase = new TestCase();

    // Create a StageInfo representing the pipeline.
    StageInfo pipelineInfo = new StageInfo();
    pipelineInfo.setStageClass("TestPipeline");
    pipelineInfo.setState(StageState.SUCCESS);
    pipelineInfo.setCounters(new ArrayList<CounterInfo>());
    pipelineInfo.setParameters(new ArrayList<StageParameter>());
    pipelineInfo.setSubStages(new ArrayList<StageInfo>());
    pipelineInfo.setModifiedParameters(new ArrayList<StageParameter>());

    Integer K = 5;
    StageParameter kParameter = new StageParameter();
    kParameter.setName("K");
    kParameter.setValue(K.toString());
    pipelineInfo.getParameters().add(kParameter);
    // Create 4 stages. The graph becomes corrupt on the third stage.
    // We use 4 stages because we want to make sure the stages are processed
    // in reverse order correctly.
    {
      BuildGraphAvro stage = new BuildGraphAvro();

      SimpleGraphBuilder builder = new SimpleGraphBuilder();
      builder.addKMersForString("ACCTGGACCTTA", K);

      String outputPath = FilenameUtils.concat(testDir, "step-0");
      StageInfo stageInfo = createStage(
          outputPath, builder.getAllNodes().values(),
          stage.getClass().getName());
      pipelineInfo.getSubStages().add(stageInfo);
    }
    {
      QuickMergeAvro stage = new QuickMergeAvro();

      SimpleGraphBuilder builder = new SimpleGraphBuilder();
      builder.addKMersForString("CTCGATAATCGA", K);

      String outputPath = FilenameUtils.concat(testDir, "step-1");
      StageInfo stageInfo = createStage(
          outputPath, builder.getAllNodes().values(),
          stage.getClass().getName());
      pipelineInfo.getSubStages().add(stageInfo);
    }
    {
      CompressAndCorrect stage = new CompressAndCorrect();

      SimpleGraphBuilder builder = new SimpleGraphBuilder();
      builder.addKMersForString("CTCGATAATCGA", K);

      // Make the graph invalid by deleting a node with edges to it.
      ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
      nodes.addAll(builder.getAllNodes().values());
      nodes.remove(0);

      String outputPath = FilenameUtils.concat(testDir, "step-2");
      StageInfo stageInfo = createStage(
          outputPath, nodes, stage.getClass().getName());
      pipelineInfo.getSubStages().add(stageInfo);
    }
    {
      RemoveTipsAvro stage = new RemoveTipsAvro();

      SimpleGraphBuilder builder = new SimpleGraphBuilder();
      builder.addKMersForString("CTAGTCGAATT", K);

      // Make the graph invalid by deleting a node with edges to it.
      ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
      nodes.addAll(builder.getAllNodes().values());
      nodes.remove(0);

      String outputPath = FilenameUtils.concat(testDir, "step-3");
      StageInfo stageInfo = createStage(
          outputPath, nodes, stage.getClass().getName());
      pipelineInfo.getSubStages().add(stageInfo);

      testCase.errorStage = stageInfo;
    }

    testCase.jsonFile = FilenameUtils.concat(testDir, "stage_info.json");

    try {
      FileOutputStream outStream = new FileOutputStream(new File(
          testCase.jsonFile));

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          pipelineInfo.getSchema(), generator);
      SpecificDatumWriter<StageInfo> writer =
          new SpecificDatumWriter<StageInfo>(StageInfo.class);
      writer.write(pipelineInfo, encoder);
      // We need to flush it.
      encoder.flush();
      outStream.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail("Couldn't create the output stream.");
    }
    return testCase;
  }

  @Test
  public void testFindLastValidGraph() {
    String testDir = FileHelper.createLocalTempDir().getPath();
    TestCase testCase = createPipeline(testDir);

    FindLastValidGraph findStage = new FindLastValidGraph();
    findStage.setConf(new Configuration());
    HashMap<String, Object> parameters = new HashMap<String, Object>();
    parameters.put("inputpath", testCase.jsonFile);
    parameters.put(
        "outputpath", FilenameUtils.concat(testDir, "validation"));
    findStage.setParameters(parameters);
    if (!findStage.execute()) {
      fail("Job failed");
    }
   assertEquals(testCase.errorStage, findStage.getErrorStage());
  }
}
