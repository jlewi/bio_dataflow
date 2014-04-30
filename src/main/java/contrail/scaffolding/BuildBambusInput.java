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
// Author: Jeremy Lewi (jeremy@lewi.us), Serge Koren(sergekoren@gmail.com)
package contrail.scaffolding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;

/**
 * This class constructs the input needed to run Bambus for scaffolding.
 *
 * The input is:
 *   1. The original reads
 *   2. The assembled contigs
 *   3. The alignments of the reads to the contigs produced by bowtie.
 *   4. A libSize file listing each library and the min/max insert size.
 *
 * The output is:
 *   1. A single fasta file containing all the original reads.
 *   2. A library file which lists the ids of each mate pair in each library.
 *   3. A tigr file containing the contigs and information about how the reads
 *      align to the contigs.
 */
public class BuildBambusInput extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(BuildBambusInput.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    definitions.putAll(super.createParameterDefinitions());

    ParameterDefinition bowtieAlignments =
        new ParameterDefinition(
            "bowtie_alignments",
            "The hdfs path to the avro files containing the alignments " +
            "produced by bowtie of the reads to the contigs.",
            String.class, null);

    ParameterDefinition libFile =
        new ParameterDefinition(
            "library_file", "A path to the json file describing the " +
            "libraries. The json file should be an array of Library " +
            "records.",
            String.class, null);

    ParameterDefinition readLength =
        new ParameterDefinition(
            "read_length",
            "How short to make the reads. The value needs to be consistent " +
            "with the value used in AlignReadsWithBowtie. Bowtie requires " +
            "short reads. Bambus needs to use the same read lengths as those " +
            "used by bowtie because otherwise there could be issues with " +
            "contig distances because read start/end coordinates for the " +
            "alignments aren't consistent.",
            Integer.class, 25);

    ParameterDefinition outputPath =
        new ParameterDefinition(
            "outputpath", "The directory to write the outputs which are " +
            "the files to pass to bambus for scaffolding. This should be " +
            "a local filesystem so that bambus can read theam.",
            String.class, null);

    ParameterDefinition outputPrefix =
        new ParameterDefinition(
            "outprefix", "The prefix for the output files defaults to " +
            "(bambus_input).",
            String.class, "bambus_input");

    ParameterDefinition hdfsPath =
        new ParameterDefinition(
            "hdfs_path", "The path on the hadoop filesystem to use as a " +
            "working directory.", String.class, null);

    ParameterDefinition graphPath =
        new ParameterDefinition(
            "graph_glob", "The glob on the hadoop filesystem to the avro " +
            "files containing the GraphNodeData records representing the " +
            "graph.", String.class, null);

    for (ParameterDefinition def:
      new ParameterDefinition[] {
        bowtieAlignments, graphPath, hdfsPath, libFile, outputPath,
        outputPrefix, readLength}) {
      definitions.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(definitions);
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> invalid = super.validateParameters();

    invalid.addAll(this.checkParameterIsNonEmptyString(Arrays.asList(
        "outputpath", "outprefix", "hdfs_path")));

    invalid.addAll(this.checkParameterMatchesFiles(Arrays.asList(
        "graph_glob", "library_file")));

    return invalid;
  }

  /** Create the fasta file for Bambus. */
  private void createFastaFile(String fastaOutputFile) {
    BuildBambusFastaFile stage = new BuildBambusFastaFile();
    stage.initializeAsChild(this);

    stage.setParameter("outputpath", fastaOutputFile);
    if (!stage.execute()) {
      sLogger.fatal(
          "Failed to create FASTA file.",
          new RuntimeException(stage.getClass().getName() + " failed."));
      System.exit(-1);

    }
  }

  /** Create the library file for Bambus. */
  private void createLibraryFile(String outputFile) {
    BuildBambusLibraryFile stage = new BuildBambusLibraryFile();
    stage.initializeAsChild(this);

    stage.setParameter("outputpath", outputFile);
    if (!stage.execute()) {
      sLogger.fatal(
          "Failed to create Library file.",
          new RuntimeException(stage.getClass().getName() + " failed."));
      System.exit(-1);

    }
  }

  /**
   * Create a tigr file from the original contigs and converted bowtie outputs.
   * @param bowtieAvroPath: Path containing the bowtie mappings in avro format.
   */
  private void createTigrFile(String bowtieAvroPath) {
    String graphPath = (String) stage_options.get("graph_glob");
    // Convert the data to a tigr file.
    TigrCreator tigrCreator = new TigrCreator();
    tigrCreator.initializeAsChild(this);

    String hdfsPath = (String)stage_options.get("hdfs_path");

    tigrCreator.setParameter(
        "inputpath", StringUtils.join(
            new String[]{bowtieAvroPath, graphPath}, ","));

    String outputPath = FilenameUtils.concat(hdfsPath, "tigr");
    tigrCreator.setParameter("outputpath", outputPath);

    if (!tigrCreator.execute()) {
      sLogger.fatal(
          "Failed to create TIGR file.",
          new RuntimeException("TigrCreator failed."));
      System.exit(-1);

    }

    // Copy tigr file to local filesystem.
    ArrayList<Path> tigrOutputs = new ArrayList<Path>();

    FileSystem fs;
    try{
      fs = FileSystem.get(this.getConf());
      for (FileStatus status : fs.listStatus(new Path(outputPath))) {
        if (status.getPath().getName().startsWith("part")) {
          tigrOutputs.add(status.getPath());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }
    if (tigrOutputs.size() != 1) {
      sLogger.fatal(String.format(
          "TigrConverter should have produced a single output file. However " +
          "%d files were found that matched 'part*' in directory %s.",
          tigrOutputs.size(), outputPath),
          new RuntimeException("Improper output."));
      System.exit(-1);
    }

    String contigOutputFile = getContigOutputFile();

    // TODO(jlewi): How can we verify that the copy completes successfully.
    try {
      fs.copyToLocalFile(
          tigrOutputs.get(0), new Path(contigOutputFile));
    } catch (IOException e) {
      sLogger.fatal(String.format(
          "Failed to copy %s to %s", tigrOutputs.get(0).toString(),
          contigOutputFile), e);
      System.exit(-1);
    }
  }

  /** Create the input for bambus. */
  @Override
  protected void stageMain() {
    String resultDir = (String) stage_options.get("outputpath");

    Path resultDirPath = new Path(resultDir);
    FileSystem fs;
    try {
      fs = resultDirPath.getFileSystem(getConf());
      if (!fs.exists(resultDirPath)) {
        sLogger.info("Creating output directory:" + resultDir);
        fs.mkdirs(resultDirPath);
      }
    } catch (IOException e) {
      sLogger.fatal("Could not create outputpath: " + resultDir, e);
    }

    String fastaOutputFile = getFastaOutputFile();
    String libraryOutputFile = getLibraryOutputFile();
    String contigOutputFile = getContigOutputFile();

    sLogger.info("Outputs will be written to:");
    sLogger.info("Fasta file: " + fastaOutputFile);
    sLogger.info("Library file: " + libraryOutputFile);
    sLogger.info("Contig Aligned file: " + contigOutputFile);

    createFastaFile(fastaOutputFile);
    createLibraryFile(libraryOutputFile);

    String bowtieAvroPath = (String) stage_options.get("bowtie_alignments");
    createTigrFile(bowtieAvroPath);
  }

  /**
   * Returns the name of the output file containing the shortened fasta reads.
   * @return
   */
  public String getFastaOutputFile() {
    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");
    return FilenameUtils.concat(resultDir, outPrefix + ".fasta");
  }

  /**
   * Returns the name of the output file containing the contigs in tigr format.
   * @return
   */
  public String getContigOutputFile() {
    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");
    return FilenameUtils.concat(resultDir, outPrefix + ".contig");
  }

  /**
   * Returns the name of the output file containing library.
   * @return
   */
  public String getLibraryOutputFile() {
    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");
    return FilenameUtils.concat(resultDir, outPrefix + ".library");
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new BuildBambusInput(), args);
    System.exit(res);
  }
}
