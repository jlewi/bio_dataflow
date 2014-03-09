/**
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
// Author: Michael Schatz, Jeremy Lewi (jeremy@lewi.us)

package contrail.stages;

import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphN50StatsData;

/**
 * Compute statistics of the contigs.
 *
 * We divide the contigs into bins based on the contig lengths such that
 * each bin contains the contigs with length L.
 *
 * The map reduce job is used to organize the contig lengths into bins and
 * to sort the bins into descending order with respect to contig lengths.
 *
 * After the map reduce job completes we process the map reduce output and
 * compute various statistics. The most important statistic being the N50
 * length. The N50 length is the length such that the sum of all contigs
 * greater than or equal to the N50 length is 50% of the sum of the lengths of
 * all contigs.
 *
 * We compute the N50 statistics for each bin. For each bin, the N50 statistics
 * are computed with respect to all contigs in that bin and the bins containing
 * longer contigs. In other words, for each bin i, we compute the N50
 * statistics using all contigs longer than L_i.
 *
 * The N50 stats are written to a separate avro file named "n50stats.avro"
 * in the output directory.
 *
 * If the option "topn_contigs" is given, then the lengths of the N largest
 * contigs will be outputted to the file "topn_contigs.avro" in the output
 * directory as well.
 */
public class GraphStats extends PipelineStage {
  private static final Logger sLogger = Logger.getLogger(GraphStats.class);
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
      {new LengthStats(), new GraphN50Stats()};

    for (StageBase stage: substages) {
      definitions.putAll(stage.getParameterDefinitions());
    }
    return Collections.unmodifiableMap(definitions);
  }

  /**
   * Compute the number of contigs of each length.
   */
  private void computeLengthStats(String inputPath, String outputPath) {
    LengthStats stage = new LengthStats();
    stage.initializeAsChild(this);
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            stage.getParameterDefinitions().values());

    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);
    if (!executeChild(stage)) {
      sLogger.fatal(
          "LengthStats failed.", new RuntimeException("LengthStats failed."));
    }
  }

  /**
   * Compute the number of N50 stats.
   */
  private void computeN50Stats(String inputPath, String outputPath) {
    GraphN50Stats stage = new GraphN50Stats();
    stage.initializeAsChild(this);
    // Make a shallow copy of the stage options required by the compress
    // stage.
    Map<String, Object> stageOptions =
        ContrailParameters.extractParameters(
            this.stage_options,
            stage.getParameterDefinitions().values());

    stageOptions.put("inputpath", inputPath);
    stageOptions.put("outputpath", outputPath);
    stage.setParameters(stageOptions);
    if (!executeChild(stage)) {
      sLogger.fatal(
          "GraphN50Stats failed.",
          new RuntimeException("GraphN50Stats failed."));
    }
  }

  /**
   * Create a CSV report to describe the result.
   */
  protected void writeReport(String statsFile, String csvFile) {
    GraphN50Stats.GraphN50StatsFileReader reader =
        new GraphN50Stats.GraphN50StatsFileReader(statsFile, getConf());

    Path csvPath = new Path(csvFile);
    try {
      FileSystem fs = csvPath.getFileSystem(getConf());
      BufferedWriter csvStream =
          new BufferedWriter(new OutputStreamWriter(fs.create(csvPath, true)));

      String[] columns = new String[] {
          "Min Length", "Max Length", "Length Sum", "N50 Length",
          "N50 Index", "Num Contigs", "Percent Total Length",
          "Percent Total Num Contigs", "Mean Coverage", "Mean Degree"
      };
      csvStream.write(StringUtils.join(columns, ",") + "\n");

      for (GraphN50StatsData record: reader) {
        HashMap<String, String> row = new HashMap<String, String>();
        row.put("Min Length", String.format("%d", record.getMinLength()));
        row.put("Max Length", String.format("%d", record.getMaxLength()));
        row.put("Length Sum", String.format("%d", record.getLengthSum()));
        row.put("N50 Length", String.format("%d", record.getN50Length()));
        row.put("N50 Index", String.format("%d", record.getN50Index()));
        row.put("Num Contigs", String.format("%d", record.getNumContigs()));
        row.put(
            "Percent Total Length",
            String.format("%f", record.getPercentLength()));
        row.put(
            "Percent Total Num Contigs",
            String.format("%f", record.getPercentNumContigs()));
        row.put("Mean Coverage", String.format("%f", record.getMeanCoverage()));
        row.put("Mean Degree", String.format("%f", record.getMeanDegree()));

        ArrayList<String> values = new ArrayList<String>();
        for (String col : columns) {
          values.add(row.get(col));
        }
        csvStream.write(StringUtils.join(values, ",") + "\n");
      }
      csvStream.close();
      sLogger.info("Wrote CVS report to: " + csvFile);
    } catch (IOException exception) {
      fail("There was a problem writing the html report. " +
          "Exception: " + exception.getMessage());
    }
  }

  @Override
  protected void stageMain() {
    String inputDir = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    String lengthsDir = FilenameUtils.concat(
        outputPath, LengthStats.class.getSimpleName());
    computeLengthStats(inputDir, lengthsDir);


    String statsFile = FilenameUtils.concat(
        outputPath, GraphN50Stats.class.getSimpleName() + ".json");
    computeN50Stats(FilenameUtils.concat(lengthsDir, "part-?????.avro"), statsFile);

    String csvFile = FilenameUtils.concat(outputPath, "report.csv");
    writeReport(statsFile, csvFile);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphStats(), args);
    System.exit(res);
  }
}
