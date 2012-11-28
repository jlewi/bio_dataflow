package contrail.scaffolding;

public class estimateMateThreshold {
        private static final double BAD_MATE_THRESHOLD = 0.25;
        private static final double EPSILON = 0.001;
        public static void main(String[] args) throws Exception {
	    String resultDir = System.getProperty("user.dir") + "/";

	    double genomeSize = Double.parseDouble(args[0]);
            long totalBases = Long.parseLong(args[1]);

System.err.println("Inputs are " + genomeSize + " " + totalBases);
            double coverage = Math.round(totalBases / genomeSize);
System.err.println("Computed coverage is " + coverage);
            int threshold = (int) Math.ceil(coverage * BAD_MATE_THRESHOLD - 0.5 - EPSILON);

System.out.println("Threshold\t" + threshold);
	}
}
