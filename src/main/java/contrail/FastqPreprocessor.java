package contrail;

import java.io.IOException;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
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
import org.apache.hadoop.mapred.lib.NLineInputFormat;


public class FastqPreprocessor extends Configured implements Tool 
{	
	private static final Logger sLogger = Logger.getLogger(FastqPreprocessor.class);
	
	
	// FastqPreprocessorMapper
	///////////////////////////////////////////////////////////////////////////
	
	private static class FastqPreprocessorMapper extends MapReduceBase 
    implements Mapper<LongWritable, Text, Text, Text> 
	{
		private int idx = 0;
		
		private String name = null;
		private String seq  = null;
		private String qv   = null;
		
		private String filename = null;
		private String suffix = null;
		private String counter = "pair_unknown";
		
		// Whether or not to time the running
		public boolean time = true;
		// Run time for the mapper in seconds.
		public Timer map_timer; 
		
		public void configure(JobConf job) 
		{
			filename = job.get("map.input.file");
			
			boolean usesuffix = Integer.parseInt(job.get("PREPROCESS_SUFFIX")) == 1;
			
			if (usesuffix)
			{
				if      (filename.contains("_1.")) { suffix = "_1"; counter = "pair_1"; }
				else if (filename.contains("_2.")) { suffix = "_2"; counter = "pair_2"; }
				else                               { counter = "pair_unpaired"; }
				
				System.err.println(filename + " suffix: \"" + suffix + "" + "\"");
			}
			
			if (time) {
			  map_timer = new Timer("FastqPreProcessor Timer");
			}
			
		}
		
		public void map(LongWritable lineid, Text line,
                OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException 
        {
		  
		  if (time) {
		    map_timer.start();
		  }
		  
			if (idx == 0) 
			{ 
				name = line.toString();

				if (name.charAt(0) != '@')
				{
					throw new IOException("ERROR: Invalid readname: " + name + " in " + filename);
				}
				
				int i = name.indexOf(' ');
				
				// strip the @ and chop after the first space
				if (i != -1) { name = name.substring(1, i); }
				else         { name = name.substring(1);    }
				
				// clean up any funny characters
				name = name.replaceAll("[:#-.|/]", "_");
				
				if (suffix != null)
				{
					name += suffix;
				}
			}
			else if (idx == 1) { seq = line.toString(); }
			else if (idx == 2) { }
			else if (idx == 3)
			{
				//qv = line.toString();
				
				output.collect(new Text(name), new Text(seq));
				
				reporter.incrCounter("Contrail", "preprocessed_reads", 1);
				reporter.incrCounter("Contrail", counter, 1);
			}
			
			idx = (idx + 1) % 4;
			
			if (time) {
			  map_timer.stop();
			}
    }
		
		public void close() throws IOException
		{
			if (idx != 0)
			{
				throw new IOException("ERROR: closing with idx = " + idx + " in " + filename);
			}
			
			if (time) {
			  System.out.println("FastqPreprocessor Map time (seconds) = " + map_timer.toSeconds());
			}
		}
	}
	
	
	
	// Run Tool
	///////////////////////////////////////////////////////////////////////////	
	
	public RunningJob run(String inputPath, String outputPath) throws Exception
	{ 
		sLogger.info("Tool name: FastqPreprocessor");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);
		
		
		JobConf conf = new JobConf(Stats.class);
		conf.setJobName("FastqPreprocessor " + inputPath);
		
		ContrailConfig.initializeConfiguration(conf);
			
		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(FastqPreprocessorMapper.class);
		conf.setNumReduceTasks(0);
		
    conf.setInputFormat(NLineInputFormat.class);
    conf.setInt("mapred.line.input.format.linespermap", 2000000); // must be a multiple of 4

    //FileOutputFormat.setCompressOutput(conf,true);
		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

    long start_time = System.currentTimeMillis();    
    RunningJob result = JobClient.runJob(conf);
    long end_time = System.currentTimeMillis();    
    double nseconds = (end_time - start_time) / 1000.0;
    System.out.println("Job took: " + nseconds + " seconds");
    
		return result;
	}
	

	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////	

	public int run(String[] args) throws Exception 
	{
		//String inputPath  = "/Users/mschatz/build/Contrail/data/Ba100k.sim.fq";
		String inputPath  = args[0];
		String outputPath = args[1];
		
		ContrailConfig.PREPROCESS_SUFFIX = 1;
		ContrailConfig.TEST_MODE = true;
		
		Timer timer = new Timer("FastqPreprocessor");
		timer.start();
		run(inputPath, outputPath);
		timer.stop();
		System.out.println("Fastqpreprocessor: Job took (seconds): " + timer.toSeconds());
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	

	public static void main(String[] args) throws Exception 
	{
		int res = ToolRunner.run(new Configuration(), new FastqPreprocessor(), args);
		System.exit(res);
	}
}
