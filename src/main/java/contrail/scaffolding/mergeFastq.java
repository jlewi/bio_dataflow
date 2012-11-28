package contrail.scaffolding;

import java.io.BufferedReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.regex.Pattern;
import java.io.PrintStream;
import java.io.File;
import java.util.HashSet;

public class mergeFastq {
  private static final NumberFormat nf = new DecimalFormat("############.#");
  public static final Pattern splitBySpaces = Pattern.compile("\\s+");
   private static final String[] fileEnds = {"fastq", "txt"};

   private HashSet<String> idsIn1 = new HashSet<String>(10000000);
   private HashSet<String> idsIn2 = new HashSet<String>(10000000);

   public mergeFastq() {
   }

   public void processFasta(String inputFile, String secondFile, String outputFile) throws Exception {
      File file1 = new File(outputFile);
      PrintStream outFile = new PrintStream(file1);
      PrintStream chaffFile = new PrintStream(outputFile.replaceAll("txt", "unmated.txt").replaceAll("fastq", "unmated"));
int chaf = 0;

System.err.println("Outputting files to " + outputFile + " and unmated " + outputFile.replaceAll("txt", "unmated.txt"));
      BufferedReader bf1 = Utils.getFile(inputFile, fileEnds);
      BufferedReader bf2 = Utils.getFile(secondFile, fileEnds);
      String line = null;
      String header = "";
      int count = 0;

      while ((line = bf1.readLine()) != null) {
         String[] split = splitBySpaces.split(line);
         header = split[0].substring(0, split[0].length()-2);
         idsIn1.add(header);
         bf1.readLine(); bf1.readLine(); bf1.readLine();
         if (count % 1000000 == 0) { System.err.println("Finished reading " + count); }
         count++;
      }
System.err.println("Done " + count);
      count = 0;
      while ((line = bf2.readLine()) != null) {
         String[] split = splitBySpaces.split(line);
         header = split[0].substring(0, split[0].length()-2);
         idsIn2.add(header);
         bf2.readLine(); bf2.readLine(); bf2.readLine();
         if (count % 1000000 == 0) { System.err.println("Finished reading " + count); }
         count++;
      }
System.err.println("Done " + count);
      bf1.close();
      bf2.close();

      count = 0;
      bf1 = Utils.getFile(inputFile, fileEnds);
      bf2 = Utils.getFile(secondFile, fileEnds);
      boolean read = true;
      while (read) {
         read=false;
         line = bf1.readLine();
         if (line != null) {
            read = true;
            String[] split = splitBySpaces.split(line);
            header = split[0].substring(0, split[0].length()-2);
            String fasta = bf1.readLine();
            String qualHeader = bf1.readLine();
            String qual = bf1.readLine();

            if (!idsIn2.contains(header)) {
//System.err.println("Header is " + header + " and header 2 " + header2);
               chaffFile.println(line);
               chaffFile.println(fasta);
               chaffFile.println(qualHeader);
               chaffFile.println(qual);
chaf++;
            }
            else {
               outFile.println(line);
               outFile.println(fasta);
               outFile.println(qualHeader);
               outFile.println(qual);
            }
         }

         line = bf2.readLine();
         if (line != null) {
            read = true;
            String[] split = splitBySpaces.split(line);
            header = split[0].substring(0, split[0].length()-2);
            String fasta = bf2.readLine();
            String qualHeader = bf2.readLine();
            String qual = bf2.readLine();

            if (!idsIn1.contains(header)) {
//System.err.println("Header is " + header + " and header 2 " + header2);
               chaffFile.println(line);
               chaffFile.println(fasta);
               chaffFile.println(qualHeader);
               chaffFile.println(qual);
chaf++;
            }
            else {
               outFile.println(line);
               outFile.println(fasta);
               outFile.println(qualHeader);
               outFile.println(qual);
            }
         }
      }

System.err.println("Done " + chaf);

      bf1.close();
      bf2.close();

      outFile.close();
   }

   public static void printUsage() {
      System.err.println("This program sizes a fasta or fastq file. Multiple fasta files can be supplied by using a comma-separated list.");
      System.err.println("Example usage: mergeFastq fasta1.fasta,fasta2.fasta");
   }

   public static void main(String[] args) throws Exception {
      if (args.length < 2) { printUsage(); System.exit(1);}

      mergeFastq f = new mergeFastq();
      f.processFasta(args[0], args[1], args[2]);
   }
}
