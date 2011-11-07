package contrail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Locale;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


/**
 * Compare the timing efficacy of different methods for preprocessing and encoding fastq files.
 * 
 * @author jlewi
 *
 */
public class TimeFastq 
{	
	private static final Logger sLogger = Logger.getLogger(TimeFastq.class);

	public String time_text(String input, String output) throws Exception{
		long start_time;
		long end_time;		

		String args[] = {input,output};
		start_time=System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new FastqPreprocessor(), args);
		end_time=System.currentTimeMillis();


		ArrayList<String> part_files = new ArrayList<String>();
		String[] files = (new File(output)).list();
		for (int index=0; index< files.length ; index++){
			String some_file=files[index];
			if (some_file.length()=="part-00000".length() && some_file.startsWith("part-")) {
				String full_path=new File(output,files[index]).getPath();				
				part_files.add(full_path);
			}
		}
		return print_output(start_time,end_time,"text",part_files);
	}
	
	public String time_avro_byte(String input, String output) throws Exception{
		long start_time;
		long end_time;		

		String args[] = {input,output};
		start_time=System.currentTimeMillis();
		int res = ToolRunner.run(new Configuration(), new FastqPreprocessorAvroByte(), args);
		end_time=System.currentTimeMillis();

		ArrayList<String> part_files = new ArrayList<String>();
		String[] files = (new File(output)).list();
		for (int index=0; index< files.length ; index++){
			String some_file=files[index];
			if (some_file.length()=="part-00000.avro".length() && some_file.startsWith("part-") && some_file.endsWith("avro")) {
				String full_path=new File(output,files[index]).getPath();				
				part_files.add(full_path);
			}
		}

		return print_output(start_time,end_time,"avro-byte", part_files);
	}
	
	private String print_output(long start_time, long end_time, String format, ArrayList<String> out_files){
		// TODO(jlewi): Compute File Size
		double nseconds = (end_time-start_time)/1000.0;		
		StringBuilder sb = new StringBuilder();

		double size=0;
		for (Iterator<String> it = out_files.iterator(); it.hasNext();){
			File file_handle = new File(it.next());
			size += file_handle.length()/1e6;	
		}

		// Send all output to the Appendable object sb
		Formatter formatter = new Formatter(sb, Locale.US);
		formatter.format("Format:%s \t Time(sec): %f \t Output Size (Mb):%f \t #Files: %d", format,nseconds,size, out_files.size());
		formatter.flush();
		System.out.println(sb.toString());
		return sb.toString();
	}


	public int run(String[] args) throws Exception 
	{
		// get stored in CommandLine.args which get passed onto ContrailConfig.parseOptions.		
		Option input = new Option("input","input",true,"The directory containing the input fastq files to process.");
		Option output = new Option("output","output",true,"The directory where the output should.");

		Options options = new Options();
		options.addOption(input);
		options.addOption(output);

		CommandLineParser parser = new GnuParser();
		CommandLine line = parser.parse( options, args, true );

		if (!line.hasOption("input") || !line.hasOption("output")){
			System.out.println("ERROR: Missing required arguments");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "TimeSerialize", options );
			return -1;
		}
		// File containing the data to parse.
		String inputPath = line.getOptionValue("input");
		String outputPath = line.getOptionValue("output");

		System.out.println("Converting fastq to modified fastq");
				
		ArrayList<String> results = new ArrayList<String>();
                
		
        results.add(time_text(inputPath,outputPath+"/text"));
        results.add(time_avro_byte(inputPath,outputPath+"/avro-byte"));		

		for (Iterator<String> it = results.iterator(); it.hasNext();){
			System.out.println(it.next());
		}
		return 0;
	}

	// Main
	///////////////////////////////////////////////////////////////////////////	
	public static void main(String[] args) throws Exception 
	{

		TimeFastq timer= new TimeFastq();
		int res = timer.run(args);
		System.exit(res);
	}
}