package contrail.scaffolding;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashMap;

public class addMissingMates {
        public static void main(String[] args) throws Exception {
	    String resultDir = System.getProperty("user.dir") + "/";

	    if (args.length < 1) {
	       System.err.println("Please provide an asm and read directory");
	       System.exit(1);
	    }

            BufferedReader libSizeFile = Utils.getFile(args[0], "libSize");
            HashMap<String, Utils.Pair> libSizes = new HashMap<String, Utils.Pair>();
            String libLine = null;
            while ((libLine = libSizeFile.readLine()) != null) {
                String[] splitLine = libLine.trim().split("\\s+");
                libSizes.put(splitLine[0], new Utils.Pair(Integer.parseInt(splitLine[1]), Integer.parseInt(splitLine[2])));
            }
            libSizeFile.close();

            PrintStream out = new PrintStream(new File(resultDir + args[0].replaceAll(".libSize", "") + ".mates"));

            for (String libName : libSizes.keySet()) {
System.err.println("The lib name is " + libName);
               out.println("library " + libName + "_paired " + libSizes.get(libName).first + " " + (int)libSizes.get(libName).second);
               Process p = Runtime.getRuntime().exec(args[2] + "/gatekeeper -dumpfragments -tabular " + args[1] + ".gkpStore");
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line = null;
            String last = null;
            int count = 0;
            while ((line = in.readLine()) != null) {
               String[] splitLine = line.trim().split("\\s+");
               if (splitLine[4].equalsIgnoreCase(libName)) {
                 if (count % 2 != 0) {
                    out.println(last + " " + splitLine[0] + " " + libName + "_paired");
                 }
                 last = splitLine[0];
                 count++;
               }
            }
            in.close();


           }
         out.close();
}
}
