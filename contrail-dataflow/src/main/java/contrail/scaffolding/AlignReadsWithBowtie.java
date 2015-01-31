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
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.scaffolding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.ContrailLogger;
import contrail.util.FileHelper;

/**
 * This stage uses bowtie to align the reads to the contigs.
 *
 * A directory on the local filesystem is used to store the output of bowtie.
 * The bowtie alignments are then converted to avro and loaded onto HDFS.
 *
 */
public class AlignReadsWithBowtie extends NonMRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(AlignReadsWithBowtie.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    definitions.putAll(super.createParameterDefinitions());

    ParameterDefinition localPath =
        new ParameterDefinition(
            "local_path",
            "A directory on the local filesystem for bowtie output.",
            String.class, null);

    ParameterDefinition hdfsPath =
        new ParameterDefinition(
            "hdfs_path", "The path on the hadoop filesystem to use for " +
            "the output.", String.class, null);

    ParameterDefinition bowtiePath =
        new ParameterDefinition(
            "bowtie_path", "The path to the bowtie binary.",
            String.class, null);

    ParameterDefinition bowtieBuildPath =
        new ParameterDefinition(
            "bowtiebuild_path", "The path to the bowtie-build binary.",
            String.class, null);

    ParameterDefinition readLength =
        new ParameterDefinition(
            "read_length", "How short to make the reads before aligning them.",
            Integer.class, 25);

    // Currently these need to be on the local filesystem.
    ParameterDefinition contigsGlob =
        new ParameterDefinition(
            "reference_glob", "A glob expression matching the path to the " +
            "fasta files containg the reference genome. Should be on the " +
            "local filesystem.",
            String.class, null);

    ParameterDefinition readsGlob =
        new ParameterDefinition(
            "reads_glob", "A glob expression matching the path to the fastq " +
            "files containg the reads to align to the reference genome. " +
            "Should be a local file system.",
            String.class, null);

    for (ParameterDefinition def:
        new ParameterDefinition[]
            {bowtiePath, bowtieBuildPath, localPath, hdfsPath, contigsGlob,
             readLength, readsGlob}) {
      definitions.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * Convert the output of bowtie to an avro file.
   *
   * @return: The path where the converted bowtie files are located.
   */
  private String convertBowtieToAvro (Collection<String> bowtieOutFiles){
    // Copy the file alignments to the hadoop filesystem so that we can
    // run mapreduce on them.
    FileSystem fs;
    try{
      fs = FileSystem.get(this.getConf());
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    sLogger.info("Create directory on HDFS for bowtie alignments.");

    String hdfsPath = (String)stage_options.get("hdfs_path");
    String hdfsAlignDir = FilenameUtils.concat(hdfsPath, "bowtie_output");

    sLogger.info("Creating hdfs directory:" + hdfsAlignDir);
    try {
      if (!fs.mkdirs(new Path(hdfsAlignDir))) {
        sLogger.fatal(
            "Could not create hdfs directory:" + hdfsAlignDir,
            new RuntimeException("Failed to create directory."));
        System.exit(-1);
      }
    } catch (IOException e) {
      sLogger.fatal(
          "Could not create hdfs directory:" + hdfsAlignDir + " error:" +
          e.getMessage(), e);
      System.exit(-1);
    }

    sLogger.info("Copy bowtie outputs to hdfs.");
    for (String bowtieFile : bowtieOutFiles) {
      String name = FilenameUtils.getName(bowtieFile);
      String newFile = FilenameUtils.concat(hdfsAlignDir, name);
      try {
        fs.copyFromLocalFile(new Path(bowtieFile), new Path(newFile));
      } catch (IOException e) {
        sLogger.fatal(String.format(
            "Could not copy %s to %s error: %s", bowtieFile, newFile,
            e.getMessage()), e);
        System.exit(-1);
      }
    }

    // Read the bowtie output.
    BowtieConverter converter = new BowtieConverter();
    converter.initializeAsChild(this);
    converter.setParameter("inputpath", hdfsAlignDir);
    converter.setParameter("outputpath", this.getBowtieOutputPath());

    if (!converter.execute()) {
      sLogger.fatal(
          "Failed to convert bowtie output to avro records.",
          new RuntimeException("BowtieConverter failed."));
      System.exit(-1);
    }

    return this.getBowtieOutputPath();
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> invalid = super.validateParameters();

    invalid.addAll(this.checkParameterIsNonEmptyString(Arrays.asList(
        "local_path", "hdfs_path")));

    invalid.addAll(this.checkParameterIsExistingLocalFile(Arrays.asList(
        "bowtie_path", "bowtiebuild_path")));

    invalid.addAll(this.checkParameterMatchesLocalFiles(Arrays.asList(
        "reference_glob", "reads_glob")));

    return invalid;
  }

  public String getBowtieOutputPath() {
    String hdfsPath = (String) stage_options.get("hdfs_path");
    String convertedPath = FilenameUtils.concat(
        hdfsPath, BowtieConverter.class.getSimpleName());
    return convertedPath;
  }

  @Override
  protected void stageMain() {
    // Run the bowtie aligner
    BowtieRunner runner = new BowtieRunner(
        (String)stage_options.get("bowtie_path"),
        (String)stage_options.get("bowtiebuild_path"));

    String localPath = (String) stage_options.get("local_path");

    String bowtieIndexDir = FilenameUtils.concat(localPath, "bowtie-index");
    String bowtieIndexBase = "index";

    String referenceGlob = (String) this.stage_options.get("reference_glob");
    ArrayList<String> contigFiles = FileHelper.matchFiles(referenceGlob);

    if (contigFiles.isEmpty()) {
      sLogger.fatal(
          "No contig files matched:"  + referenceGlob,
          new RuntimeException("Missing inputs."));
      System.exit(-1);
    }

    if (!runner.bowtieBuildIndex(
        contigFiles, bowtieIndexDir, bowtieIndexBase)) {
      sLogger.fatal(
          "There was a problem building the bowtie index.",
          new RuntimeException("Failed to build bowtie index."));
      System.exit(-1);
    }

    ArrayList<String> readFiles = FileHelper.matchListOfGlobs(
          (String)stage_options.get("reads_glob"));
    if (readFiles.isEmpty()) {
      sLogger.fatal(
          "No read files matched:"  +
          (String) this.stage_options.get("reads_glob"),
          new RuntimeException("Missing inputs."));
      System.exit(-1);
    }

    Integer readLength = (Integer) stage_options.get("read_length");
    String alignDir = FilenameUtils.concat(localPath, "bowtie-alignments");
    BowtieRunner.AlignResult alignResult = runner.alignReads(
        bowtieIndexDir, bowtieIndexBase, readFiles, alignDir,
        readLength);

    convertBowtieToAvro(alignResult.outputs.values());
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AlignReadsWithBowtie(), args);
    System.exit(res);
  }
}
