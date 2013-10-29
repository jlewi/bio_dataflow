package contrail;


public class ContrailConfig
{
	// important paths
	public static String hadoopReadPath = null;
	public static String hadoopBasePath = null;
	public static String localBasePath = "work";

	// hadoop options
	public static int    HADOOP_MAPPERS    = 50;
	public static int    HADOOP_REDUCERS   = 50;
	public static int    HADOOP_LOCALNODES = 1000;
	public static long   HADOOP_TIMEOUT    = 3600000;
	public static String HADOOP_JAVAOPTS   = "-Xmx1000m";

	// Assembler options
	public static String STARTSTAGE = null;
	public static String STOPSTAGE = null;

	// restart options
	public static boolean validateonly = false;
	public static boolean forcego = false;
	public static boolean RESTART_USED = false;
	public static int RESTART_INITIAL = 0;
	public static int RESTART_TIP = 0;
	public static int RESTART_TIP_REMAIN = 0;
	public static int RESTART_COMPRESS = 0;
	public static int RESTART_COMPRESS_REMAIN = 0;

	public static String RESTART_SCAFF_STAGE = null;
	public static long   RESTART_SCAFF_FRONTIER = 0;
	public static int    RESTART_SCAFF_PHASE = 0;

	// initial node construction
	public static long K = -1;
	public static long MAXR5 = 250;
	public static long MAXTHREADREADS = 250;
	public static int  RECORD_ALL_THREADS = 0;

	// hard trim
	public static long TRIM3 = 0;
	public static long TRIM5 = 0;

	// tips
	public static long TIPLENGTH = -3;

	// bubbles
	public static long MAXBUBBLELEN    = -5;
	public static float BUBBLEEDITRATE = 0.05f;

	// low cov
	public static float LOW_COV_THRESH  = 5.0f;
	public static long  MAX_LOW_COV_LEN = -2;

	// threads
	public static long  MIN_THREAD_WEIGHT = 5;

    // scaffolding
	public static long  INSERT_LEN     = 0;
	public static long  MIN_WIGGLE     = 30;
	public static float MIN_UNIQUE_COV = 0;
	public static float MAX_UNIQUE_COV = 0;
	public static long  MIN_CTG_LEN    = -1;
	public static long  MAX_FRONTIER   = 10;

	// stats
	public static String RUN_STATS = null;
	public static long  N50_TARGET = -1;

    // conversion
	public static String CONVERT_FA = null;
	public static String PRINT_FA = null;
	public static long FASTA_MIN_LEN = 100;
	public static float FASTA_MIN_COV = 2;

	// preprocess
	public static long   PREPROCESS_SUFFIX = 0;


	public static boolean TEST_MODE = false;
}
