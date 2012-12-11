package contrail.scaffolding;

import java.io.BufferedReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class SizeFasta {
  private static final NumberFormat nf = new DecimalFormat("############.#");
   private boolean ungapped = false;

   public SizeFasta() {
   }

   /**
    * Get the length of the sequence.
    *
    * If the member variable ungapped is true then we count gap characters
    * in the sequence. Otherwise we don't.
    *
    * @param fastaSeq
    * @return
    */
   private int getFastaStringLength(StringBuffer fastaSeq) {
      return (ungapped == false ? fastaSeq.length() : fastaSeq.toString().replaceAll("N", "").replaceAll("n", "").replaceAll("-", "").length());
   }

   /**
    * Read the fasta file and print out the length of each fasta sequence.
    * @param inputFile
    * @throws Exception
    */
   public void processFasta(String inputFile) throws Exception {
      BufferedReader bf = Utils.getFile(inputFile, "fasta");

      String line = null;
      StringBuffer fastaSeq = new StringBuffer();
      String header = "";

      while ((line = bf.readLine()) != null) {
         if (line.startsWith(">")) {
            if (fastaSeq.length() != 0) {System.out.println(header + "\t" + getFastaStringLength(fastaSeq)); }
            header = line.substring(1);
            fastaSeq = new StringBuffer();
         }
         else {
            fastaSeq.append(line);
         }
      }

      if (fastaSeq.length() != 0) {
        System.out.println(header + "\t" + getFastaStringLength(fastaSeq));
      }
      bf.close();
   }

   /**
    * Read the fastq file and print out the length of each fastq sequence.
    * @param inputFile
    * @throws Exception
    */
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
