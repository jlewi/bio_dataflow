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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
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
    Stage[] substages =
      {new LengthStats(), new GraphN50Stats()};

    for (Stage stage: substages) {
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
   * Create an HTML report to describe the result.
   */
  protected void writeReport(String statsFile, String reportFile) {
    GraphN50Stats.GraphN50StatsFileReader reader =
        new GraphN50Stats.GraphN50StatsFileReader(statsFile, getConf());

    // We create a temporary local file to write the data to.
    // We then copy that file to the output path which could be on HDFS.
    // TODO(jlewi): Why can't we just write to HDFS directly?
    File temp = null;
    try {
      temp = File.createTempFile("temp",null);
    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
    }

    String outputDir = (String) stage_options.get("outputpath");
    Path outputPath = new Path(reportFile);

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    try {
      FileWriter fileWriter = new FileWriter(temp);
      BufferedWriter writer = new BufferedWriter(fileWriter);

      //writer.create(schema, outputStream);
      writer.append("<html><body>");
      writer.append("Path:" + statsFile);
      writer.append("<br><br>");
      writer.append("N50 Statistics");
      writer.append("<table border=1>");
      writer.append(
          "<tr><td>Min Length</td><td>Max Length</td><td>Length Sum</td>" +
          "<td>N50 Length</td><td>N50 Index</td><td>Num Contigs</td>" +
          "<td>Mean Coverage</td><td>MeanDegree</td></tr>");
      for (GraphN50StatsData record: reader) {
        writer.append(String.format("<td>%d</td>", record.getMinLength()));
        writer.append(String.format("<td>%d</td>", record.getMaxLength()));
        writer.append(String.format("<td>%d</td>", record.getLengthSum()));
        writer.append(String.format("<td>%d</td>", record.getN50Length()));
        writer.append(String.format("<td>%d</td>", record.getN50Index()));
        writer.append(String.format("<td>%d</td>", record.getNumContigs()));
        writer.append(String.format("<td>%f</td>", record.getMeanCoverage()));
        writer.append(String.format("<td>%f</td>", record.getMeanDegree()));
        writer.append("<tr>");
      }
      writer.append("</table>");
      writer.append("</body></html>");
      writer.close();
      fs.moveFromLocalFile(new Path(temp.toString()), outputPath);

      sLogger.info("Wrote HTML report to: " + outputPath.toString());
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

    String reportFile = FilenameUtils.concat(outputPath, "report.html");
    writeReport(statsFile, reportFile);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new GraphStats(), args);
    System.exit(res);
  }
}
