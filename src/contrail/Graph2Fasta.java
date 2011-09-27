package contrail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Graph2Fasta extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(Graph2Fasta.class);
	
	private static int LINE_LEN = 60;
	
	// Graph2FastaMapper
	///////////////////////////////////////////////////////////////////////////
	
	private static class Graph2FastaMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		public static long FASTA_MIN_LEN = 0;
		public static float FASTA_MIN_COV = 0.0f;
		
		public void configure(JobConf job) 
		{
			FASTA_MIN_LEN = Long.parseLong(job.get("FASTA_MIN_LEN"));
			FASTA_MIN_COV = Float.parseFloat(job.get("FASTA_MIN_COV"));
		}
		
		public void map(LongWritable lineid, Text nodetxt,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException 
        {
			Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());

			reporter.incrCounter("Contrail", "allnodes", 1);
			
			String str = node.str();
			int len = str.length();
			float cov = node.cov();
			
			if ((len >= FASTA_MIN_LEN) && (cov >= FASTA_MIN_COV))
			{
				reporter.incrCounter("Contrail", "printednodes", 1);
			
				output.collect(new Text(">" + node.getNodeId()), new Text("len=" + str.length() + "\tcov=" + node.cov()));
			
				for (int i = 0; i < str.length(); i+=LINE_LEN)
				{
					int end = i + LINE_LEN;
					if (end > str.length()) { end = str.length();}
				
					String line = str.substring(i, end);
					output.collect(new Text(line), null);
				}
			}
        }
	}

	// convertGraph - read graph from file, print to stdout
	///////////////////////////////////////////////////////////////////////////	

	public void printFasta(String inputPath) throws Exception
	{
		BufferedReader in  = new BufferedReader(new FileReader(inputPath));
		
		String nodetxt = in.readLine();
		while (nodetxt != null)
		{
			Node node = new Node();
			node.fromNodeMsg(nodetxt);
			
			String str = node.str();
			int len = str.length();
			float cov = node.cov();
			
			if ((len >= ContrailConfig.FASTA_MIN_LEN) && (cov >= ContrailConfig.FASTA_MIN_COV))
			{
				System.out.println(">" + node.getNodeId() + "\tlen=" + str.length() + "\tcov=" + node.cov());
			
				for (int i = 0; i < str.length(); i+=LINE_LEN)
				{
					int end = i + LINE_LEN;
					if (end > str.length()) { end = str.length();}
				
					String line = str.substring(i, end);
					System.out.println(line);
				}
			}
		
			nodetxt = in.readLine();
		}
	}

	
	// Run Tool
	///////////////////////////////////////////////////////////////////////////	
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: Graph2Fasta");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("Graph2Fasta " + inputPath);
		
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Graph2FastaMapper.class);

		ContrailConfig.initializeConfiguration(conf);
		conf.setNumReduceTasks(0);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}
	

	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		//String inputPath  = "/users/mschatz/contrail/Ec100k/99-final/";
		//String outputPath = "/users/mschatz/contrail/Ec100k/99-final.fa/";

		String inputPath  = "/users/mschatz/contrail/Ec100k/04-notipscmp";
		String outputPath = "/users/mschatz/contrail/Ec100k/04-notipscmp.fa";
		
		ContrailConfig.FASTA_MIN_LEN = 100;
		ContrailConfig.FASTA_MIN_COV = 2.0f;
		
		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new Graph2Fasta(), args);
		System.exit(res);
	}
}
