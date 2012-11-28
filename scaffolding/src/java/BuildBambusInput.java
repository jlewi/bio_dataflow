import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

/**
 * This class constructs the input needed to run Bambus for scaffolding.
 *
 * The input is the original reads and the assembled contigs. The reads
 * are aligned to the contigs using the Bowtie aligner. The aligned reads
 * and contigs are then outputted in the appropriate format for use with
 * Bambus.
 */
public class BuildBambusInput {
  private static class MappingInfo {
    public String readID = null;

    // position on the read
    int start = 0;
    int end = 0;

    // position on the contig
    int contigStart = 0;
    int contigEnd = 0;
  }

  private static final int SUB_LEN = 25;
  private static final int NUM_READS_PER_CTG = 200;
  private static final int NUM_CTGS = 200000;

  /**
   * Build the bowtie index by invoking bambus.
   *
   * @param bowtiePath
   * @param contigFiles
   * @param outBase
   */
  protected static void bowtieBuildIndex(
      String bowtiePath, Collection<String> contigFiles, String outBase)
          throws Exception{
    Process p = Runtime.getRuntime().exec("");
    p.waitFor();
  }

  /**
   * Read the bowtie output.
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
        while (!splitLine[position].equalsIgnoreCase("+") && !splitLine[position].equalsIgnoreCase("-")) {
          position++;
        }

        String contigID = splitLine[position+1];
        Boolean isFwd = null;
        if (splitLine[position].contains("-")) {
          isFwd = false;
        } else if (splitLine[position].contains("+")) {
          isFwd = true;
        }

        m.readID = prefix + splitLine[0].replaceAll("/", "_");
        m.start = 1;
        m.end = SUB_LEN;
        m.contigStart = Integer.parseInt(splitLine[position+2]);
        if (isFwd) {
          m.contigEnd = m.contigStart + splitLine[position+3].length() - 1;
        } else {
          m.contigEnd = m.contigStart;
          m.contigStart = m.contigEnd + splitLine[position+3].length() - 1;
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

  private static void outputContigRecord(PrintStream out, String contigID, String sequence, ArrayList<MappingInfo> reads) {
    out.println("##" + contigID + " " + (reads == null ? 0 : reads.size()) + " 0 bases 00000000 checksum.");
    out.print(sequence);
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

  // An array containing a list of the readFiles;
  private File[] readFiles;

  /**
   * Parse the library file and extract the library sizes.
   */
  private void parseLibSizes(String libFile) throws Exception {
    libSizes = new HashMap<String, Utils.Pair>();
    BufferedReader libSizeFile = Utils.getFile(libFile, "libSize");
    String libLine = null;
    while ((libLine = libSizeFile.readLine()) != null) {
      String[] splitLine = libLine.trim().split("\\s+");
      libSizes.put(splitLine[0], new Utils.Pair(Integer.parseInt(splitLine[1]), Integer.parseInt(splitLine[2])));
    }
    libSizeFile.close();
  }

  /**
   * Find the prefixes of all the FASTQ files.
   */
  private void findReadPrefixes() {
    // Process the original read files.
    File dir = new File(readDir);
    readFiles = dir.listFiles();
    prefixes = new HashSet<String>();

    // Find FASTQ files containing mate pairs and extract the filename prefix.
    for (File fs : readFiles) {
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
    System.err.println("Prefixes for files I will read are " + prefixes);
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
      File fastaOutputFile) throws Exception {
    PrintStream out = new PrintStream(fastaOutputFile);
    HashMap<String, HashMap<String, ArrayList<String>>> mates =
        new HashMap<String, HashMap<String, ArrayList<String>>>();
    for (File fs : readFiles) {
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
              System.err.println("Processed " + counter + " reads");
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
   * Align the contigs to the reads.
   * @param args
   * @throws Exception
   */
  public void build(String[] args) throws Exception {
    String resultDir = System.getProperty("user.dir") + "/";
    if (args.length < 3) {
      System.err.println("Please provide an asm and read directory");
      System.exit(1);
    }

    String execPath = BuildBambusInput.class.getClassLoader().getResource(BuildBambusInput.class.getName().replace('.', File.separatorChar) + ".class").getPath();
    String perlCommand = new File(execPath).getParent() + File.separatorChar + "get_singles.pl";

    // First argument is the assembly directory.
    assemblyDir = args[0];
    // Second argument is the directory containing the original reads.
    readDir = args[1];
    String suffix = args[2];
    String libFile = args[3];

    // All output files will start with outPrefix. The suffix depends on
    // the type of file written.
    String outPrefix = args[4];

    System.err.println("Arguments are:");
    System.err.println("assemblyDir: " + assemblyDir);
    System.err.println("readDir: " + readDir);
    System.err.println("suffix: " + suffix);
    System.err.println("libFile: " + libFile);
    System.err.println("outprefix: " + outPrefix);

    parseLibSizes(libFile);

    File dir = new File(assemblyDir);
    if (!dir.isDirectory()) {
      System.err.println(
          "Error, assembly directory " + assemblyDir + " is not a directory");
      System.exit(1);
    }

    dir = new File(readDir);
    if (!dir.isDirectory()) {
      System.err.println("Error, read directory " + readDir + " is not a directory");
      System.exit(1);
    }

    findReadPrefixes();

    File fastaOutputFile = new File(resultDir + outPrefix + ".fasta");
    File libraryOutputFile = new File(resultDir + outPrefix + ".library");
    File contigOutputFile = new File(resultDir + outPrefix + ".contig");

    System.err.println("Outputs will be written to:");
    System.err.println("Fasta file: " + fastaOutputFile.getName());
    System.err.println("Library file: " + libraryOutputFile.getName());
    System.err.println("Contig Aligned file: " + contigOutputFile.getName());

    HashMap<String, HashMap<String, ArrayList<String>>> mates =
        shortenReads(fastaOutputFile);

    createLibraryFile(libraryOutputFile, mates);
    System.err.println("Library file built");

    // Run the bowtie aligner
    System.err.println("Launching bowtie aligner: " + perlCommand + " -reads " + readDir + " -assembly " + assemblyDir + " -suffix " + suffix + " --threads 2");
    Process p = Runtime.getRuntime().exec("perl " + perlCommand + " -reads " + readDir + " -assembly " + assemblyDir + " -suffix " + suffix + " --threads 2");
    p.waitFor();
    System.err.println("Bowtie finished");
    HashMap<String, ArrayList<MappingInfo>> map = new HashMap<String, ArrayList<MappingInfo>>(NUM_CTGS);
    for (String prefix : prefixes) {
      String first = resultDir + prefix.replaceAll("X", "1") + ".bout";
      String second  = resultDir + prefix.replaceAll("X", "2") + ".bout";
      if (!new File(first).exists()) {
        first = first + ".bz2";
        second = second + ".bz2";
        if (!new File(first).exists()) {
          System.err.println("Cannot find bowtie output, expected " + resultDir + prefix + ".1.bout[.bz2]");
          System.exit(1);
        }
      }
      readBowtieResults(first, prefix, map);
      readBowtieResults(second, prefix, map);
    }

    // finally run through all the contig files and build the TIGR .contig file
    dir = new File(assemblyDir);
    if (!dir.isDirectory()) {
      System.err.println("Error, read directory " + assemblyDir + " is not a directory");
      System.exit(1);
    }

    // TODO(jlewi): This code will only read contigs from the first contig
    // file that matches. Is this reading the original contig files, or
    // contigOutputFile.
    File contigFasta = null;
    for (File f: dir.listFiles()) {
      if (f.getName().endsWith(suffix)) {
        contigFasta = f;
        break;
      }
    }

    PrintStream out = new PrintStream(contigOutputFile);
    BufferedReader bf = new BufferedReader(new FileReader(contigFasta));
    String line = null;
    String contigID = null;
    StringBuffer contigSequence = null;
    int counter = 0;
    while ((line = bf.readLine()) != null) {
      String[] splitLine = line.trim().split("\\s+");
      if (splitLine[0].startsWith(">")) {
        if (contigID != null) {
          if (counter % 10000 == 0) {
            System.err.println("Processed in " + counter + " contig records");
          }
          counter++;

          outputContigRecord(out, contigID, contigSequence.toString(), map.get(contigID));
        }
        contigID = splitLine[0].replaceAll(">", "");
        contigSequence = new StringBuffer();
      } else {
        contigSequence.append(line + "\n");
      }
    }

    if (contigID != null) {
      outputContigRecord(out, contigID, contigSequence.toString(), map.get(contigID));
    }

    bf.close();
    out.close();
  }

  public static void main(String[] args) throws Exception {
    BuildBambusInput builder = new BuildBambusInput();
    builder.build(args);
  }
}
