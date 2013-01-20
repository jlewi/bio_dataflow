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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastaRecord;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import contrail.util.FileHelper;

/**
 * This class constructs the input needed to run Bambus for scaffolding.
 *
 * The input is the original reads and the assembled contigs. The reads
 * are aligned to the contigs using the Bowtie aligner. The aligned reads
 * and contigs are then outputted in the appropriate format for use with
 * Bambus.
 */
public class BuildBambusInput extends Stage {
  private static final Logger sLogger =
      Logger.getLogger(BuildBambusInput.class);
  private static final int SUB_LEN = 25;

  private File fastaOutputFile;
  private File libraryOutputFile;
  private File contigOutputFile;

  /**
   * This class stores a pair of files containing mate pairs.
   */
  private static class MateFilePair {
    public String leftFile;
    public String rightFile;
    public String libraryName;

    public MateFilePair(
        String libraryName, String leftFile, String rightFile) {
      this.libraryName = libraryName;
      this.leftFile = leftFile;
      this.rightFile = rightFile;
    }
  }

  /**
   * Store the minimum and maximum size for inserts in a library.
   */
  private static class LibrarySize {
    final public int minimum;
    final public int maximum;
    private int libSize;

    public LibrarySize(int first, int second) {
      minimum = Math.min(first, second);
      maximum = Math.max(first, second);
      libSize = maximum - minimum + 1;
    }

    public int size() {
      return libSize;
    }
  }

  // libSizes stores the sizes for each read library. The key is the
  // prefix of the FastQ files for that library. The value is a pair
  // which stores the lower and uppoer bound for the library size.
  private HashMap<String, LibrarySize> libSizes;

  /**
   * Parse the library file and extract the library sizes.
   */
  private void parseLibSizes(String libFile) throws Exception {
    libSizes = new HashMap<String, LibrarySize>();
    BufferedReader libSizeFile =
        new BufferedReader(new FileReader(new File(libFile)));
    String libLine = null;
    while ((libLine = libSizeFile.readLine()) != null) {
      String[] splitLine = libLine.trim().split("\\s+");
      libSizes.put(
          splitLine[0],
          new LibrarySize(
              Integer.parseInt(splitLine[1]),
              Integer.parseInt(splitLine[2])));
    }
    libSizeFile.close();
  }

  /**
   * Given a set of read files, group them into mate pairs.
   *
   * @param readFiles
   */
  private ArrayList<MateFilePair> buildMatePairs(Collection<String> readFiles) {
    HashMap<String, ArrayList<String>> libraryFiles =
        new HashMap<String, ArrayList<String>>();

    ArrayList<MateFilePair> matePairs = new ArrayList<MateFilePair>();
    // Group the reads into mate pairs. Each mate pair belongs to a library.
    // The library name is given by the prefix of the filename.
    for (String filePath : readFiles) {
      String name = FilenameUtils.getBaseName(filePath);
      // We expect the filename to be something like "libraryName_1.fastq"
      // or "libraryName_2.fastq".
      if (!name.matches((".*_[012]"))) {
        sLogger.fatal(
            "File: " + filePath + " doesn't match the patern .*_[012] so we " +
            "couldn't determine the library name",
            new RuntimeException());
      }
      // We want to strip off the trailing _[12]
      String libraryName = name.substring(0, name.length() - 2);

      if (!libraryFiles.containsKey(libraryName)) {
        libraryFiles.put(libraryName, new ArrayList<String>());
      }
      libraryFiles.get(libraryName).add(filePath);
    }

    for (String libraryName : libraryFiles.keySet()) {
      ArrayList<String> files = libraryFiles.get(libraryName);
      if (files.size() != 2) {
        String message =
            "There was a problem grouping the reads into mate pairs. Each " +
            "library (filename prefix) should match two files. But for " +
            "library:" + libraryName + " the number of matching files was:" +
            files.size() + ".";
        if (files.size() > 0) {
          message =
              "The files that matched were: " + StringUtils.join(files, ",");
        }
        sLogger.fatal(message, new RuntimeException(message));
      }
      MateFilePair pair = new MateFilePair(
          libraryName, files.get(0), files.get(1));
      matePairs.add(pair);

      sLogger.info("Found mate pairs for library:" + libraryName);
    }
    return matePairs;
  }

  /**
   * Create an index of mate pairs for each library and write fasta output.
   *
   * @param: The file to write the truncated reads to.
   * @return: A hash map of the mate pairs for each library.
   *  The key for the hash map is the prefix for the library and identifies
   *  a set of mate pairs.
   *  The value is a hashmap with two keys "left" and "right". Each key
   *  stores one set of reads in the mate pairs for this library. The value
   *  of "left" and "right" is an array of strings storing the ids of all
   *  the reads. Thus
   *  mates[prefix]["left"][i] and mates[prefix]["right"][i] should be the
   *  id's of the the i'th mate pair in the library given by prefix.
   *
   * The code assumes that the reads in two mate pair files are already
   * aligned. i.e The i'th record in frag_1.fastq is the mate pair for
   * the i'th record in frag_2.fastq
   *
   * This function also processes all the reads, and truncates the reads to
   * length SUB_LEN. All of the truncated reads are then written to
   * fastaOutputFile. The reads are truncated because BOWTIE is a short
   * read aligner.
   */
  private HashMap<String, HashMap<String, ArrayList<String>>> shortenReads(
      Collection<MateFilePair> matePairs,
      File fastaOutputFile) throws Exception {
    sLogger.info(
        String.format(
            "Shortening the reads to align to %d bases and writing them " +
             "to: %s", SUB_LEN, fastaOutputFile.getPath()));
    PrintStream out = new PrintStream(fastaOutputFile);
    HashMap<String, HashMap<String, ArrayList<String>>> mates =
        new HashMap<String, HashMap<String, ArrayList<String>>>();

    FastaRecord fastaRecord = new FastaRecord();

    final int NUM_READS = 200;
    for (MateFilePair matePair : matePairs) {
      // first trim to 25bp
      sLogger.info(
          "Processing reads for library:" + matePair.libraryName);
      mates.put(matePair.libraryName, new HashMap<String, ArrayList<String>>());
      mates.get(matePair.libraryName).put(
          "left", new ArrayList<String>(NUM_READS));
      mates.get(matePair.libraryName).put(
          "right", new ArrayList<String>(NUM_READS));

      for (int i = 0; i < 2; ++i) {
        String readFile = null;
        ArrayList<String> readIds = null;
        if (i == 0) {
          readFile = matePair.leftFile;
          readIds = mates.get(matePair.libraryName).get("left");
        } else {
          readFile = matePair.rightFile;
          readIds = mates.get(matePair.libraryName).get("right");
        }
        FastQFileReader reader = new FastQFileReader(readFile);

        int counter = 0;
        while (reader.hasNext()) {
          FastQRecord record = reader.next();

          // Prefix the id by the libraryName.
          // TODO(jeremy@lewi.us): The original code add the library name
          // as a prefix to the read id and then replaced "/" with "_".
          // I think manipulating the readId's is risky because we need to
          // be consistent. So we don't prepend the library name.
          // However, some programs e.g bowtie cut the "/" off and set a
          // a special code. So to be consistent we use the function
          // safeReadId to convert readId's to a version that can be safely
          // used everywhere.
          fastaRecord.setId(Utils.safeReadId(record.getId().toString()));

          // Truncate the read because bowtie can only handle short reads.
          fastaRecord.setRead(record.getRead().subSequence(0, SUB_LEN));

          out.println(">" +  fastaRecord.getId());
          out.println(fastaRecord.getRead());

          readIds.add(fastaRecord.getId().toString());

          ++counter;
          if (counter % 1000000 == 0) {
            sLogger.info("Processed " + counter + " reads");
            out.flush();
          }
          counter++;
        }
      }
    }
    out.close();
    return mates;
  }

  /**
   * Create a library file.
   * The library file lists each mate pair in each ibrary.
   *
   * @param libraryOutputFile: The file to write to.
   * @param mates: A hash map specifying the mate pairs in each library.
   *
   * For each library fetch the library size or throw an error if there
   * is no size for this library.
   * Write out the library file. The library is a text file.
   * For each library there is a line starting wtih "library" which
   * contains the name of the library and the size for the library.
   * For each mate pair in the library we write a line with the id's
   * of the reads forming the pair and the name of the library they come
   * from.
   */
  private void createLibraryFile(
      File libraryOutputFile,
      HashMap<String, HashMap<String, ArrayList<String>>> mates)
      throws Exception {
    PrintStream libOut = new PrintStream(libraryOutputFile);
    for (String lib : mates.keySet()) {
      HashMap<String, ArrayList<String>> libMates = mates.get(lib);
      String libName = lib.replaceAll("_", "");
      if (libSizes.get(libName) == null) {
        String knownLibraries = "";
        for (String library : libSizes.keySet()) {
          knownLibraries += library + ",";
        }
        // Strip the last column.
        knownLibraries = knownLibraries.substring(
            0, knownLibraries.length() - 1);
        sLogger.fatal(
            "No library sizes are defined for libray:" + libName + " . Known " +
            "libraries are: " + knownLibraries,
            new RuntimeException("No library sizes for libray:" + libName));
      }
      libOut.println(
          "library " + libName + " " + libSizes.get(libName).minimum + " " +
          libSizes.get(libName).maximum);
      ArrayList<String> left = libMates.get("left");
      ArrayList<String> right = libMates.get("right");

      if (left.size() != right.size()) {
        sLogger.fatal(
            "Not all reads in library " + libName + " have a mat.",
            new RuntimeException("Not all reads are paired."));
      }

      for (int whichMate = 0; whichMate < left.size(); whichMate++) {
        // If the left read name starts with "l", "p" or "#" because
        // the binary toAmos_new in the amos package reserves uses these
        // characters to identify special types of rows in the file.
        String leftRead = left.get(whichMate);
        if (leftRead.startsWith("l") || leftRead.startsWith("p") ||
            leftRead.startsWith("#")) {
          sLogger.fatal(
              "The read named:" + leftRead + " will cause problems with the " +
              "amos binary toAmos_new. The amos binary attributes special " +
              "meaning to rows in the library file starting with 'p', 'l' or " +
              "'#' so if the id for a read starts with any of those " +
              "characters it will mess up amos.",
              new RuntimeException("Invalid read name"));
          System.exit(-1);
        }
        libOut.println(
            left.get(whichMate) + " " + right.get(whichMate) + " " + libName);
      }
    }
    libOut.close();
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
    HashMap<String, Object> convertOptions = new HashMap<String, Object>();

    String convertedPath = FilenameUtils.concat(
        hdfsPath, BowtieConverter.class.getSimpleName());
    convertOptions.put("inputpath", hdfsAlignDir);
    convertOptions.put("outputpath", convertedPath);

    converter.setConf(getConf());
    converter.setParameters(convertOptions);

    try {
      RunningJob convertJob = converter.runJob();
      if (!convertJob.isSuccessful()) {
        sLogger.fatal(
            "Failed to convert bowtie output to avro records.",
            new RuntimeException("BowtieConverter failed."));
        System.exit(-1);
      }
    } catch (Exception e) {
      sLogger.fatal("Failed to convert bowtie outputs to avro.", e);
      System.exit(-1);
    }
    return convertedPath;
  }

  /**
   * Create a tigr file from the original contigs and converted bowtie outputs.
   * @param bowtieAvroPath: Path containing the bowtie mappings in avro format.
   */
  private void createTigrFile(String bowtieAvroPath) {
    String graphPath = (String) stage_options.get("graph_glob");
    // Convert the data to a tigr file.
    TigrCreator tigrCreator = new TigrCreator();
    HashMap<String, Object> tigrOptions = new HashMap<String, Object>();

    String hdfsPath = (String)stage_options.get("hdfs_path");

    tigrOptions.put(
        "inputpath", StringUtils.join(
            new String[]{bowtieAvroPath, graphPath}, ","));

    String outputPath = FilenameUtils.concat(hdfsPath, "tigr");

    tigrOptions.put("outputpath", outputPath);

    tigrCreator.setConf(getConf());
    tigrCreator.setParameters(tigrOptions);

    try {
      RunningJob tigrJob = tigrCreator.runJob();
      if (!tigrJob.isSuccessful()) {
        sLogger.fatal(
            "Failed to create TIGR file.",
            new RuntimeException("TigrCreator failed."));
        System.exit(-1);
      }
    } catch (Exception e) {
      sLogger.fatal("Failed to create TIGR file.", e);
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

    // TODO(jlewi): How can we verify that the copy completes successfully.
    try {
      fs.copyToLocalFile(
          tigrOutputs.get(0), new Path(contigOutputFile.getPath()));
    } catch (IOException e) {
      sLogger.fatal(String.format(
          "Failed to copy %s to %s", tigrOutputs.get(0).toString(),
          contigOutputFile), e);
      System.exit(-1);
    }
  }

  /**
   * Align the contigs to the reads.
   *
   * @param args
   * @throws Exception
   */
  public void build() throws Exception {
    String libFile = (String) this.stage_options.get("libsize");
    parseLibSizes(libFile);

    ArrayList<String> readFiles = FileHelper.matchFiles(
        (String) this.stage_options.get("reads_glob"));

    if (readFiles.isEmpty()) {
      sLogger.fatal(
          "No read files matched:"  +
          (String) this.stage_options.get("reads_glob"),
          new RuntimeException("Missing inputs."));
      System.exit(-1);
    }

    sLogger.info("Files containing reads to align are:");
    for (String file : readFiles) {
      sLogger.info("read file:" + file);
    }
    ArrayList<MateFilePair> matePairs = buildMatePairs(readFiles);

    String referenceGlob = (String) this.stage_options.get("reference_glob");
    ArrayList<String> contigFiles = FileHelper.matchFiles(referenceGlob);

    if (contigFiles.isEmpty()) {
      sLogger.fatal(
          "No contig files matched:"  + referenceGlob,
          new RuntimeException("Missing inputs."));
      System.exit(-1);
    }

    sLogger.info("Files containing contings to align reads to are:");
    for (String file : contigFiles) {
      sLogger.info("contig file:" + file);
    }

    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");

    File resultDirFile = new File(resultDir);
    if (!resultDirFile.exists()) {
      sLogger.info("Creating output directory:" + resultDir);

      if (!resultDirFile.mkdirs()) {
        sLogger.fatal("Could not create directory:" + resultDir);
        System.exit(-1);
      }
    }
    fastaOutputFile = new File(resultDir, outPrefix + ".fasta");
    libraryOutputFile = new File(resultDir, outPrefix + ".library");
    contigOutputFile = new File(resultDir, outPrefix + ".contig");

    sLogger.info("Outputs will be written to:");
    sLogger.info("Fasta file: " + fastaOutputFile.getName());
    sLogger.info("Library file: " + libraryOutputFile.getName());
    sLogger.info("Contig Aligned file: " + contigOutputFile.getName());

    HashMap<String, HashMap<String, ArrayList<String>>> mates =
        shortenReads(matePairs, fastaOutputFile);

    createLibraryFile(libraryOutputFile, mates);
    sLogger.info("Library file written:" + libraryOutputFile.getPath());

    // Run the bowtie aligner
    BowtieRunner runner = new BowtieRunner(
        (String)stage_options.get("bowtie_path"),
        (String)stage_options.get("bowtiebuild_path"));

    String bowtieIndexDir = FilenameUtils.concat(resultDir, "bowtie-index");
    String bowtieIndexBase = "index";
    if (!runner.bowtieBuildIndex(
        contigFiles, bowtieIndexDir, bowtieIndexBase)) {
      sLogger.fatal(
          "There was a problem building the bowtie index.",
          new RuntimeException("Failed to build bowtie index."));
      System.exit(-1);
    }

    String alignDir = FilenameUtils.concat(resultDir, "bowtie-alignments");
    BowtieRunner.AlignResult alignResult = runner.alignReads(
        bowtieIndexDir, bowtieIndexBase, readFiles, alignDir,
        SUB_LEN);

    String bowtieAvroPath = convertBowtieToAvro(alignResult.outputs.values());

    createTigrFile(bowtieAvroPath);
  }

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    definitions.putAll(super.createParameterDefinitions());

    ParameterDefinition bowtiePath =
        new ParameterDefinition(
            "bowtie_path", "The path to the bowtie binary.",
            String.class, null);

    ParameterDefinition bowtieBuildPath =
        new ParameterDefinition(
            "bowtiebuild_path", "The path to the bowtie-build binary.",
            String.class, null);

    ParameterDefinition readsGlob =
        new ParameterDefinition(
            "reads_glob", "A glob expression matching the path to the fastq " +
            "files containg the reads to align to the reference genome.",
            String.class, null);

    ParameterDefinition contigsGlob =
        new ParameterDefinition(
            "reference_glob", "A glob expression matching the path to the " +
            "fasta files containg the reference genome.",
            String.class, null);

    ParameterDefinition libsizePath =
        new ParameterDefinition(
            "libsize", "The path to the file containing the sizes for each " +
            "library",
            String.class, null);

    ParameterDefinition outputPath =
        new ParameterDefinition(
            "outputpath", "The directory to write the outputs which are " +
            "the files to pass to bambus for scaffolding.",
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
        bowtiePath, bowtieBuildPath, readsGlob, contigsGlob, libsizePath,
        outputPath, outputPrefix, hdfsPath, graphPath}) {
      definitions.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * Returns the name of the output file containing the shortened fasta reads.
   * @return
   */
  public File getFastaOutputFile() {
    return fastaOutputFile;
  }

  /**
   * Returns the name of the output file containing the contigs in tigr format.
   * @return
   */
  public File getContigOutputFile() {
    return contigOutputFile;
  }

  /**
   * Returns the name of the output file containing library.
   * @return
   */
  public File getLibraryOutputFile() {
    return libraryOutputFile;
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {
        "bowtie_path", "bowtiebuild_path", "reads_glob", "reference_glob",
        "libsize", "outputpath", "hdfs_path", "graph_glob"};
    checkHasParametersOrDie(required_args);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }
    this.setConf(conf);
    initializeJobConfiguration(conf);
    build();
    return null;
  }

  public static void main(String[] args) throws Exception {
    BuildBambusInput stage = new BuildBambusInput();
    int res = stage.run(args);
    System.exit(res);
  }
}
