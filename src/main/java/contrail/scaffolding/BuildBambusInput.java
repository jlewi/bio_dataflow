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
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;

import contrail.scaffolding.BowtieRunner.MappingInfo;
import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;
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
  private static final int NUM_READS_PER_CTG = 200;
  private static final int NUM_CTGS = 200000;

  /**
   * Read the bowtie output.
   *
   * For information about the output format of bowtie see:
   * http://bowtie-bio.sourceforge.net/manual.shtml#output
   *
   * @param fileName
   * @param prefix
   * @param map: This is a hashmap keyed by the id of each contig. The value
   *   is an array of MappingInfo. Each MappingInfo stores information about
   *   a read aligned to contig given by the key.
   * @throws Exception
   */
  private static void readBowtieResults(String fileName, String prefix, HashMap<String, ArrayList<MappingInfo>> map) throws Exception {
    prefix = prefix.replaceAll("X.*", "");
    System.err.println("For file " + fileName + " prefix is " + prefix);
    BufferedReader bf = Utils.getFile(fileName, "bout");
    if (bf != null) {
      String line = null;
      int counter = 0;
      while ((line = bf.readLine()) != null) {
        if (counter % 1000000 == 0) {
          System.err.println("Read " + counter + " mapping records from " + fileName);
        }
        String[] splitLine = line.trim().split("\\s+");
        MappingInfo m = new MappingInfo();

        int position = 1;
        // skip crud
        // The first field in the output is the name of the read that was
        // aligned. The second field is a + or - indicating which strand
        // the read aligned to. We identify the position of the +/- in
        // the split line and use this to determine the mapping of output
        // fields to indexes in splitLine.
        while (!splitLine[position].equalsIgnoreCase("+") &&
               !splitLine[position].equalsIgnoreCase("-")) {
          position++;
        }

        String readID = splitLine[position - 1];
        String strand = splitLine[position];
        String contigID = splitLine[position + 1];

        // 0-based offset into the forward reference strand where leftmost
        // character of the alignment occurs.
        String forwardOffset = splitLine[position + 2];
        String readSequence = splitLine[position + 3];

        // isFWD indicates the read was aligned to the forward strand of
        // the reference genome.
        Boolean isFwd = null;
        if (strand.contains("-")) {
          isFwd = false;
        } else if (strand.contains("+")) {
          isFwd = true;
        } else {
          throw new RuntimeException("Couldn't parse the alignment strand");
        }

        // The first field in the output is the readId. We prefix this
        // with information about which file the read came from.
        m.readID = prefix + readID.replaceAll("/", "_");
        m.start = 1;
        // TODO(jeremy@lewi.us): Need to check whether the length should be
        // zero based or 1 based. The original code set this to SUB_LEN
        // which was the length of the truncated reads which were aligned.
        //m.end = readSequence.length();
        // TODO(jerem@lewi.US): Do we have to pass in SUB_LEN or can we determine
        // it from the output.
        m.end = SUB_LEN;


        m.contigStart = Integer.parseInt(forwardOffset);
        if (isFwd) {
          m.contigEnd = m.contigStart + readSequence.length() - 1;
        } else {
          m.contigEnd = m.contigStart;
          m.contigStart = m.contigEnd + readSequence.length() - 1;
        }

        if (map.get(contigID) == null) {
          map.put(contigID, new ArrayList<MappingInfo>(NUM_READS_PER_CTG));
        }
        map.get(contigID).add(m);
        counter++;
      }
      bf.close();
      // Print out the final record count.
      System.err.println("Read " + counter + " mapping records from " + fileName);
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

  private static boolean containsPrefix(HashSet<String> prefix, String name, String postfix) {
    boolean contains = false;

    for (String s : prefix) {
      System.err.println("Checking for " + s.replaceAll("X", ".") + "." + postfix);
      if (name.matches(s.replaceAll("X", ".") + "." + postfix)) {
        contains = true;
        break;
      }
    }
    return contains;
  }

  // libSizes stores the sizes for each read library. The key is the
  // prefix of the FastQ files for that library. The value is a pair
  // which stores the lower and uppoer bound for the library size.
  private HashMap<String, Utils.Pair> libSizes;

  // A list of prefixes for the FASTQ files containing the original reads.
  HashSet<String> prefixes;

  private String assemblyDir;
  private String readDir;

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
   * Find the prefixes of all the FASTQ files.
   */
  private void findReadPrefixes(Collection<String> readFiles) {
    prefixes = new HashSet<String>();

    // Find FASTQ files containing mate pairs and extract the filename prefix.
    for (String filePath : readFiles) {
      File fs = new File(filePath);
      // TODO(jlewi): Why do we ignore files with SRR in their name?
      if (fs.getName().contains("SRR")) { continue; }
      // TODO(jeremy@lewi.us): This regular expression can match temporary
      // files e.g files that begin and end with '#'.
      if (fs.getName().matches(".*_[12]\\..*fastq.*")) {
        prefixes.add(fs.getName().replaceAll("\\.fastq", "").replaceAll(
            "\\.bz2", "").replaceAll(
                "1\\.", "X.").replaceAll(
                    "2\\.", "X.").replaceAll(
                        "1$", "X").replaceAll(
                            "2$", "X"));
      }
    }
    sLogger.info("Prefixes for files I will read are " + prefixes);
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
      Collection<String> readFiles, File fastaOutputFile) throws Exception {
    sLogger.info(
        String.format(
            "Shortening the reads to align to %d bases and writing them " +
             "to: %s", SUB_LEN, fastaOutputFile.getPath()));
    PrintStream out = new PrintStream(fastaOutputFile);
    HashMap<String, HashMap<String, ArrayList<String>>> mates =
        new HashMap<String, HashMap<String, ArrayList<String>>>();
    for (String readFile : readFiles) {
      File fs = new File(readFile);
      // first trim to 25bp
      // TODO(jlewi): It looks like the operands of the or operator are the
      // same rendering the or meaningless.
      if (containsPrefix(prefixes, fs.getName(), "fastq")) {
        String myPrefix = fs.getName().replaceAll("1\\.", "X").replaceAll("2\\.", "X").replaceAll("X.*", "");
        System.err.println("Processing file " + fs.getName() + " prefix " + myPrefix + " FOR FASTA OUTPUT");
        if (mates.get(myPrefix) == null) {
          mates.put(myPrefix, new HashMap<String, ArrayList<String>>());
          mates.get(myPrefix).put("left", new ArrayList<String>());
          mates.get(myPrefix).put("right", new ArrayList<String>());
        }
        BufferedReader bf = Utils.getFile(fs.getAbsolutePath(), "fastq");
        if (bf != null) {
          String line = null;
          int counter = 0;

          while ((line = bf.readLine()) != null) {
            if (counter % 4 == 0) {
              String name = line.replaceAll("@", ""+myPrefix).replaceAll("/", "_");
              out.println(">" + name);
              String[] split = name.trim().split("\\s+");
              if (fs.getName().matches(".*1\\..*")) {
                mates.get(myPrefix).get("left").add(split[0]);
              } else if (fs.getName().matches(".*2\\..*")) {
                mates.get(myPrefix).get("right").add(split[0]);
              }
            } else if ((counter - 1) % 4 == 0) {
              out.println(line.substring(0, SUB_LEN));
            }
            if (counter % 1000000 == 0) {
              sLogger.info("Processed " + counter + " reads");
              out.flush();
            }
            counter++;
          }
          bf.close();
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
        System.err.println("No library sizes defined for library:" + libName);
        String knownLibraries = "";
        for (String library : libSizes.keySet()) {
          knownLibraries += library + ",";
        }
        // Strip the last column.
        knownLibraries = knownLibraries.substring(
            0, knownLibraries.length() - 1);
        System.err.println("Known libraries are: " + knownLibraries);
        System.exit(1);
      }
      libOut.println("library " + libName + " " + libSizes.get(libName).first + " " + (int)libSizes.get(libName).second);
      ArrayList<String> left = libMates.get("left");
      ArrayList<String> right = libMates.get("right");
      for (int whichMate = 0; whichMate < left.size(); whichMate++) {
        libOut.println(left.get(whichMate) + " " + right.get(whichMate) + " " + libName);
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
//    // First argument is the assembly directory.
//    assemblyDir = args[0];
//    // Second argument is the directory containing the original reads.
//    readDir = args[1];
//    String suffix = args[2];
//    String libFile = args[3];
//
//    // All output files will start with outPrefix. The suffix depends on
//    // the type of file written.
//    String outPrefix = args[4];
//
//    System.err.println("Arguments are:");
//    System.err.println("assemblyDir: " + assemblyDir);
//    System.err.println("readDir: " + readDir);
//    System.err.println("suffix: " + suffix);
//    System.err.println("libFile: " + libFile);
//    System.err.println("outprefix: " + outPrefix);

    String libFile = (String) this.stage_options.get("libsize");

    parseLibSizes(libFile);

//    File dir = new File(assemblyDir);
//    if (!dir.isDirectory()) {
//      System.err.println(
//          "Error, assembly directory " + assemblyDir + " is not a directory");
//      System.exit(1);
//    }
//
//    dir = new File(readDir);
//    if (!dir.isDirectory()) {
//      System.err.println("Error, read directory " + readDir + " is not a directory");
//      System.exit(1);
//    }

    ArrayList<String> readFiles = matchFiles(
        (String) this.stage_options.get("reads_glob"));

    sLogger.info("Files containing reads to align are:");
    for (String file : readFiles) {
      sLogger.info("read file:" + file);
    }

    ArrayList<String> contigFiles = matchFiles(
        (String) this.stage_options.get("reference_glob"));

    sLogger.info("Files containing contings to align reads to are:");
    for (String file : contigFiles) {
      sLogger.info("contig file:" + file);
    }

    findReadPrefixes(readFiles);

    String resultDir = (String) stage_options.get("outputpath");
    String outPrefix = (String) stage_options.get("outprefix");
    File fastaOutputFile = new File(resultDir + outPrefix + ".fasta");
    File libraryOutputFile = new File(resultDir + outPrefix + ".library");
    File contigOutputFile = new File(resultDir + outPrefix + ".contig");

    sLogger.info("Outputs will be written to:");
    sLogger.info("Fasta file: " + fastaOutputFile.getName());
    sLogger.info("Library file: " + libraryOutputFile.getName());
    sLogger.info("Contig Aligned file: " + contigOutputFile.getName());

    HashMap<String, HashMap<String, ArrayList<String>>> mates =
        shortenReads(readFiles, fastaOutputFile);

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
          "There was a problem building the bowtie index.");

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

    return Collections.unmodifiableMap(definitions);
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
