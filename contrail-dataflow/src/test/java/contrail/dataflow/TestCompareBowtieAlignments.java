package contrail.dataflow;

import static org.junit.Assert.assertEquals;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.scaffolding.BowtieMapping;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestCompareBowtieAlignments {
  Random generator = new Random();

  private static BowtieMapping emptyMapping() {
    BowtieMapping mapping = new BowtieMapping();
    mapping.setContigId("");
    mapping.setContigStart(0);
    mapping.setContigEnd(0);
    mapping.setNumMismatches(0);
    mapping.setRead("");
    mapping.setReadClearEnd(0);
    mapping.setReadClearStart(0);
    mapping.setReadId("");

    return mapping;
  }

  private static class TestCase {
    public ArrayList<BowtieMapping> shortAlignments;
    public ArrayList<BowtieMapping> fullAlignments;
    HashMap<String, TableRow> expected;
    Random generator;

    TestCase() {
      generator = new Random();
      shortAlignments = new ArrayList<BowtieMapping>();
      fullAlignments = new ArrayList<BowtieMapping>();

      expected = new HashMap<String, TableRow>();
      for (int i = 0; i < 2; ++i) {
        String readId = "Read-" + i;
        BowtieMapping shortMapping = emptyMapping();
        shortMapping.setReadId(readId);
        String shortStrand = null;
        if (generator.nextFloat() < .5) {
          shortMapping.setContigStart(0);
          shortMapping.setContigEnd(10);
          shortStrand = "F";
        } else {
          shortMapping.setContigStart(10);
          shortMapping.setContigEnd(0);
          shortStrand = "R";
        }
        shortAlignments.add(shortMapping);

        BowtieMapping fullMapping = emptyMapping();
        fullMapping.setReadId(readId);
        String fullStrand = null;
        if (generator.nextFloat() < .5) {
          fullMapping.setContigStart(0);
          fullMapping.setContigEnd(10);
          fullStrand = "F";
        } else {
          fullMapping.setContigStart(10);
          fullMapping.setContigEnd(0);
          fullStrand = "R";
        }
        fullAlignments.add(fullMapping);

        TableRow row = new TableRow();
        row.set("read_id", readId);
        row.set("full-strand", fullStrand);
        row.set("short-strand", shortStrand);

        expected.put(readId, row);
      }
    }
  }

  @Test
  public void testJoin() {
    // TODO(jeremy@lewi.us): Need to update this test to use the TestPipeline
    // class.
    TestCase testData = new TestCase();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    DataflowUtil.registerAvroCoders(p);

    PCollection<BowtieMapping> shortCollection = p.begin().apply(
        Create.of(testData.shortAlignments));

    PCollection<BowtieMapping> fullCollection = p.begin().apply(Create.of(
        testData.fullAlignments));

    CompareBowtieAlignments stage = new CompareBowtieAlignments();

    PCollection<TableRow> joined = stage.buildPipeline(p,
        fullCollection, shortCollection);

    PipelineResult pipelineResult = p.run();
    EvaluationResults result = (EvaluationResults) pipelineResult;
    List<TableRow> finalResults = result.getPCollection(joined);

    assertEquals(2, finalResults.size());

    HashMap<String, TableRow> results = new HashMap<String, TableRow>();

    for (TableRow row : finalResults) {
      results.put((String)row.get("read_id"), row);
    }

    assertEquals(testData.expected, results);
  }

  @Test
  public void testFullFilterPipeline() throws FileNotFoundException {
    // TODO(jeremy@lewi.us): This doesn't actually work because the code
    // doesn't support local paths. We need to move the paths onto GCS.
    TestCase testData = new TestCase();

    String localDir = FileHelper.createLocalTempDir().getPath();
    String fullFile = FilenameUtils.concat(localDir, "short.avro");
    AvroFileUtil.writeRecords(
        new Configuration(), new Path(fullFile), testData.fullAlignments);

    String shortFile = FilenameUtils.concat(localDir, "full.txt");
    PrintStream outStream = new PrintStream(shortFile);
    for (BowtieMapping mapping : testData.fullAlignments) {
      ArrayList<String> fields = new ArrayList<String>();
      fields.add(mapping.getReadId().toString());
      String strand = null;
      if (mapping.getContigStart() < mapping.getContigEnd()) {
        strand = "+";
      } else {
        strand = "-";
      }
      fields.add(strand);
      fields.add("contig");
      fields.add(Integer.toString(Math.min(
          mapping.getContigStart(), mapping.getContigEnd())));
      fields.add(AlphabetUtil.randomString(
          generator, 10, DNAAlphabetFactory.create()));

      outStream.println(StringUtils.join(" ", fields));
    }

    System.out.println("TempDir:" + localDir);

    return;
//    outStream.close();
//
//    CompareBowtieAlignments stage = new CompareBowtieAlignments();
//    stage.setParameter("runner", "DirectPipelineRunner");
//    stage.setParameter("full_alignments", fullFile);
//    stage.setParameter("short_alignments",  shortFile);
//    stage.setParameter("output", "biocloudops:speciesA.CompareReads");
//    stage.setParameter("stageinfopath", "/tmp/stageinfo");
//    stage.setParameter("project", "biocloudops");
//    stage.setParameter("dataflowEndpoint","");
//    stage.setParameter("apiRootUrl", "");
//    stage.setParameter("stagingLocation", "");
//    assertTrue(stage.execute());
  }
}
