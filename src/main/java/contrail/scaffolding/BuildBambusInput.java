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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import contrail.scaffolding.BowtieRunner.MappingInfo;
import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

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
   * Outputs a contig record in tigr format.
   *
   * tigr format includes the contig and information about how reads align
   * to that contig.
   *
   * @param out
   * @param contigID
   * @param sequence
   * @param reads
   */
  private static void outputContigRecord(
      PrintStream out, FastaRecord contig, ArrayList<MappingInfo> reads) {
    out.println("##" + contig.getId() + " " + (reads == null ? 0 : reads.size()) + " 0 bases 00000000 checksum.");
    out.print(contig.getRead());
    if (reads != null) {
      for (MappingInfo m : reads) {
        out.println("#" + m.readID + "(" + Math.min(m.contigEnd, m.contigStart) + ") " + (m.contigEnd >= m.contigStart ? "[]" : "[RC]") + " " + (m.end - m.start + 1) + " bases, 00000000 checksum. {" + " " + (m.contigEnd >= m.contigStart ? m.start + " " + m.end : m.end + " " + m.start) + "} <" + (m.contigEnd >= m.contigStart ? (m.contigStart+1) + " " + (m.contigEnd+1) : (m.contigEnd+1) + " " + (m.contigStart+1)) + ">");
      }
    }
  }

  // libSizes stores the sizes for each read library. The key is the
  // prefix of the FastQ files for that library. The value is a pair
  // which stores the lower and uppoer bound for the library size.
  private HashMap<String, Utils.Pair> libSizes;

  /**
   * Parse the library file and extract the library sizes.
   */
  private void parseLibSizes(String libFile) throws Exception {
    libSizes = new HashMap<String, Utils.Pair>();
    BufferedReader libSizeFile =
        new BufferedReader(new FileReader(new File(libFile)));
    String libLine = null;
    while ((libLine = libSizeFile.readLine()) != null) {
      String[] splitLine = libLine.trim().split("\\s+");
      libSizes.put(
          splitLine[0],
          new Utils.Pair(
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
          // be consistent. For example, if we alter the read here, it won't
          // agree with the read ids in the bowtie output.
          fastaRecord.setId(record.getId());

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
          "library " + libName + " " + libSizes.get(libName).first + " " +
          (int)libSizes.get(libName).second);
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
   * Find files matching the glob expression.
   * @param glob
   * @return
   */
  private ArrayList<String> matchFiles(String glob) {
    // We assume glob is a directory + a wild card expression
    // e.g /some/dir/*.fastq
    File dir = new File(FilenameUtils.getFullPath(glob));
    String pattern = FilenameUtils.getName(glob);
    FileFilter fileFilter = new WildcardFileFilter(pattern);

    File[] files =  dir.listFiles(fileFilter);
    ArrayList<String> result = new ArrayList<String>();

    if (files.length == 0) {
      return result;
    }

    for (File file : files) {
      result.add(file.getPath());
    }
    return result;
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

    ArrayList<String> readFiles = matchFiles(
        (String) this.stage_options.get("reads_glob"));

    sLogger.info("Files containing reads to align are:");
    for (String file : readFiles) {
      sLogger.info("read file:" + file);
    }
    ArrayList<MateFilePair> matePairs = buildMatePairs(readFiles);

    ArrayList<String> contigFiles = matchFiles(
        (String) this.stage_options.get("reference_glob"));

    sLogger.info("Files containing contings to align reads to are:");
    for (String file : contigFiles) {
      sLogger.info("contig file:" + file);
    }

    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");

    fastaOutputFile = new File(resultDir + outPrefix + ".fasta");
    libraryOutputFile = new File(resultDir + outPrefix + ".library");
    contigOutputFile = new File(resultDir + outPrefix + ".contig");

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
    }

    String alignDir = FilenameUtils.concat(resultDir, "bowtie-alignments");
    BowtieRunner.AlignResult alignResult = runner.alignReads(
        bowtieIndexDir, bowtieIndexBase, readFiles, alignDir);

    HashMap<String, ArrayList<BowtieRunner.MappingInfo>> alignments =
        runner.readBowtieResults(alignResult.outputs.values(), SUB_LEN);
    // Finally run through all the contig files and build the TIGR .contig file

    PrintStream tigrOut= new PrintStream(contigOutputFile);

    for (String contigFile : contigFiles) {
      int counter = 0;
      sLogger.info("Writing tigr output for contig file:" + contigFile);
      FastaFileReader reader = new FastaFileReader(contigFile);
      while (reader.hasNext()) {
        ++counter;
        if (counter % 10000 == 0) {
          sLogger.info("Processed in " + counter + " contig records.");
        }
        FastaRecord contig = reader.next();

        outputContigRecord(tigrOut, contig, alignments.get(contig.getId()));
      }

      reader.close();
    }

    tigrOut.close();
  }

  /**
   * Get the parameters used by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

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

    for (ParameterDefinition def:
      new ParameterDefinition[] {
        bowtiePath, bowtieBuildPath, readsGlob, contigsGlob, libsizePath,
        outputPath, outputPrefix}) {
      definitions.put(def.getName(), def);
    }

    ParameterDefinition logFile = ContrailParameters.getCommonMap().get(
        "log_file");
    ParameterDefinition help = ContrailParameters.getCommonMap().get(
        "help");
    definitions.put(logFile.getName(), logFile);
    definitions.put(help.getName(), help);
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
        "libsize", "outputpath"};
    checkHasParametersOrDie(required_args);
    build();
    return null;
  }

  public static void main(String[] args) throws Exception {
    BuildBambusInput stage = new BuildBambusInput();
    int res = stage.run(args);
    System.exit(res);
  }
}
