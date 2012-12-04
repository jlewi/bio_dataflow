package contrail.scaffolding;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;

public class SubFasta {
  private static final NumberFormat nf = new DecimalFormat("############.#");

  private static class Position {
      public int start;
      public int end;
      public String name = null;

      public Position() {
      }

      public Position(int s, int e, String n) {
         start = s;
         end = e;
         name = n;
      }
   }

   private static final int MAX_READ = 10000;

   private HashMap<Integer, ArrayList<Double>> positionQual = new HashMap<Integer, ArrayList<Double>>(MAX_READ);
   private HashMap<String, Position> fastaToOutput = new HashMap<String, Position>();
   private HashMap<String, Boolean> outputIDs = new HashMap<String, Boolean>();
   private int minValue = Integer.MAX_VALUE;
   private int maxValue = Integer.MIN_VALUE;

   public SubFasta() {
   }

   public void inputIDs(String file) throws Exception {
      if (file == null) {
         return;
      }
      String line = null;
      BufferedReader bf = new BufferedReader(new InputStreamReader(
            new FileInputStream(file)));
      while ((line = bf.readLine()) != null) {
         String[] split = line.trim().split("\\s+");

         try {
            if (split.length < 3) {
               throw new Exception("Insufficient number of arguments");
            }
            Position p = new Position();
            p.start = Integer.parseInt(split[1])-1;
            p.end = Integer.parseInt(split[2]);

            if (split.length > 3) {
              p.name = split[3];
            }
            fastaToOutput.put(split[0], p);
         } catch (Exception e) {
           System.err.println("Invalid line " + e.getLocalizedMessage() + " for line " + line);
         }
      }
      bf.close();
   }

   public void outputFasta(String fastaSeq, String ID) {
      outputFasta(fastaSeq, null, ID, ">", null, true);
   }

   public void outputFasta(String fastaSeq, String qualSeq, String ID, String fastaSeparator, String qualSeparator, boolean convert) {
      if (fastaSeq.length() == 0) {
         return;
      }

      if (qualSeq != null && qualSeq.length() != fastaSeq.length()) {
         System.err.println("Error length of sequences and fasta for id " + ID + " aren't equal fasta: " + fastaSeq.length() + " qual: " + qualSeq.length());
         System.exit(1);
      }

      if (fastaToOutput.size() == 0 || fastaToOutput.get(ID) != null) {
         if (outputIDs.get(ID) != null && outputIDs.get(ID) == true) {
            return;
         }

         outputIDs.put(ID, true);
         Position p = fastaToOutput.get(ID);
         if (p == null) { p = new Position(); p.start = 0; p.end = fastaSeq.length(); }
         if (p.start < 0) { p.start = 0; }
         if (p.end == 0 || p.end > fastaSeq.length()) { System.err.println("FOR ID " + ID + " ADJUSTED END"); p.end = fastaSeq.length(); }

         System.out.println(fastaSeparator + (p.name == null ? ID : p.name));
         System.out.println((convert == true ? Utils.convertToFasta(fastaSeq.substring(p.start, p.end)) : fastaSeq.substring(p.start, p.end)));

         if (qualSeq != null) {
            System.out.println(qualSeparator + (p.name == null ? ID : p.name));
            System.out.println((convert == true ? Utils.convertToFasta(qualSeq.substring(p.start, p.end)) : qualSeq.substring(p.start, p.end)));
         }
      }
   }

   public void processFasta(String inputFile) throws Exception {
      BufferedReader bf = Utils.getFile(inputFile, "fasta");

      String line = null;
      StringBuffer fastaSeq = new StringBuffer();
      String header = "";

      while ((line = bf.readLine()) != null) {
         if (line.startsWith(">")) {
            outputFasta(fastaSeq.toString(), header);
            header = line.trim().split("\\s+")[0].substring(1);
            fastaSeq = new StringBuffer();
         }
         else {
            fastaSeq.append(line);
         }
      }

      outputFasta(fastaSeq.toString(), header);
      bf.close();
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
         //outputFasta(fasta, qualSeq, ID, "@", "+", false);
         outputFasta(fasta, ID);
      }

      bf.close();
   }

   public static void printUsage() {
      System.err.println("This program subsets a fasta or fastq file by a specified list. The default sequence is N. Multiple fasta files can be supplied by using a comma-separated list.");
      System.err.println("Example usage: SubFasta subsetFile fasta1.fasta,fasta2.fasta");
   }

   public static void main(String[] args) throws Exception {
      if (args.length < 1) { printUsage(); System.exit(1);}

      SubFasta f = new SubFasta();
      if (args.length < 2) {
         printUsage();
         System.exit(1);
      }

      try {
         f.inputIDs(args[0]);
      } catch (Exception e) {
         // do nothing ignore errors in IDs
      }
      String[] splitLine = args[1].trim().split(",");
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
