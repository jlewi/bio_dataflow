package contrail.scaffolding;

import java.io.BufferedReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;

import org.apache.log4j.Logger;

import contrail.sequences.FastaFileReader;
import contrail.sequences.FastaRecord;

public class SizeFasta {
  private static final Logger sLogger = Logger.getLogger(
      AssembleScaffolds.class);
  private static final NumberFormat nf = new DecimalFormat("############.#");
   private boolean ungapped = false;

   public SizeFasta() {
   }

   private int getFastaStringLength(String fastaSeq) {
      return (ungapped == false ? fastaSeq.length() : fastaSeq.toString().replaceAll("N", "").replaceAll("n", "").replaceAll("-", "").length());
   }

   public HashMap<String, Integer> processFasta(String inputFile) throws Exception {
      FastaFileReader reader = new FastaFileReader(inputFile);

      HashMap<String, Integer> sizes = new HashMap<String, Integer>();
      while (reader.hasNext()) {
        FastaRecord record = reader.next();
        if (sizes.containsKey(record.getId().toString())) {
          sLogger.fatal("Duplicate read id:" + record.getId(), new RuntimeException("Duplicate ID"));
          System.exit(-1);
        }
        sizes.put(record.getId().toString(), getFastaStringLength(record.getRead().toString()));
      }

      return sizes;
   }

   public void processFastq(String inputFile) throws Exception {
      BufferedReader bf = Utils.getFile(inputFile, "fastq");

      String line = null;
      StringBuffer fastaSeq = new StringBuffer();
      String header = "";

      while ((line = bf.readLine()) != null) {
         // read four lines at a time for fasta, qual, and headers
         String ID = line.split("\\s+")[0].substring(1);
         String fasta = bf.readLine();
         String qualID = bf.readLine().split("\\s+")[0].substring(1);

         if (qualID.length() != 0 && !qualID.equals(ID)) {
            System.err.println("Error ID " + ID + " DOES not match quality ID " + qualID);
            System.exit(1);
         }
         String qualSeq = bf.readLine();
         System.out.println(header + "\t" + fasta.length());
      }

      bf.close();
   }

   public static void printUsage() {
      System.err.println("This program sizes a fasta or fastq file. Multiple fasta files can be supplied by using a comma-separated list.");
      System.err.println("Example usage: SizeFasta fasta1.fasta,fasta2.fasta");
   }

   public static void main(String[] args) throws Exception {
      if (args.length < 1) { printUsage(); System.exit(1);}

      SizeFasta f = new SizeFasta();
      if (args.length >= 2) { f.ungapped = Boolean.parseBoolean(args[1]); }

      String[] splitLine = args[0].trim().split(",");
      for (int j = 0; j < splitLine.length; j++) {
System.err.println("Processing file " + splitLine[j]);
     	  if (splitLine[j].contains("fasta")) {
             f.processFasta(splitLine[j]);
          } else if (splitLine[j].contains("fastq")) {
             f.processFastq(splitLine[j]);
          } else {
             System.err.println("Unknown file type " + splitLine[j]);
          }
       }
   }
}
