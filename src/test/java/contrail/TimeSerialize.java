package contrail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;


/**
 * Compare the timing efficacy of different serialization methods.
 * 
 * @author jlewi
 *
 */
public class TimeSerialize 
{	
	private static final Logger sLogger = Logger.getLogger(TimeSerialize.class);

	/**
	 * Read a text file containing serialized nodes.
	 * 
	 * @param args
	 * @return List of the read nodes
	 * @throws Exception
	 */
	private ArrayList<Node> ReadTextFile(String path ) throws java.io.IOException{		
		// Read in the nodes
		ArrayList<Node> nodes = new ArrayList<Node>();

		// Read in the file it should be a text file.
		FileReader file_reader=new FileReader(path);
		BufferedReader buffer_reader= new BufferedReader(file_reader);

		String line=buffer_reader.readLine();

		while (line != null){ 
			// parse the node
			// create a new instance otherwise we will overwrite previous results
			Node input_node= new Node();
			input_node.fromNodeMsg(line);
			nodes.add(input_node);
			line=buffer_reader.readLine();
		}
		buffer_reader.close();
		file_reader.close();
		return nodes;
	}

	/**
	 * Read a text file containing serialized nodes. This is only intended to be for timing
	 * 
	 * @param args
	 * @return List of the read nodes
	 * @throws Exception
	 */
	private void ReadTextFileToTime(String path ) throws java.io.IOException{		
		// Read in the file it should be a text file.
		FileReader file_reader=new FileReader(path);
		BufferedReader buffer_reader= new BufferedReader(file_reader);

		String line=buffer_reader.readLine();

		while (line != null){ 
			// parse the node
			// create a new instance otherwise we will overwrite previous results
			Node input_node= new Node();
			input_node.fromNodeMsg(line);
			//don't store the nodes as we'll run out of memory
			//nodes.add(input_node);
			line=buffer_reader.readLine();
		}
		buffer_reader.close();
		file_reader.close();		
	}
	
	/**
	 * Read the nodes stored in an Avro file
	 * 
	 * @param args
	 * @return List of the read nodes
	 * @throws Exception
	 */
	private void ReadAvroFile(String path) throws java.io.IOException{		
		NodeData node_data = new NodeData();
		// Read in the file it should be a text file.
		DataFileReader<NodeData> file_reader=new DataFileReader<NodeData>(new File(path),new SpecificDatumReader(node_data.getSchema()));		

		while (file_reader.hasNext()){
			//Don't save them we run out of memory
			//nodes.add(new NodeAvro(file_reader.next()));
			new NodeAvro(file_reader.next());
		}		
		
		file_reader.close();
		
	}
	/**
	 * Write a bunch of nodes to a text file.
	 * 
	 * @param args
	 * @return List of the read nodes
	 * @throws Exception
	 */
	private void WriteTextFile(String path, ArrayList<Node> nodes) throws java.io.IOException{					
		// Read in the file it should be a text file.
		FileWriter file_writer=new FileWriter(path);
		BufferedWriter buffer_writer= new BufferedWriter(file_writer);

		Iterator<Node> it = nodes.iterator();		
		Node node = null;

		while (it.hasNext()){
			node = it.next();			
			buffer_writer.write(node.toNodeMsg(true));
		}
		buffer_writer.close();
		file_writer.close();		
	}

	private void WriteAvroFile(String path, ArrayList<NodeAvro> nodes, int compression) throws java.io.IOException {
		// write the avro version
		NodeData node_data= new NodeData();
		DataFileWriter<NodeData> data_writer =  new DataFileWriter<NodeData>(new SpecificDatumWriter(node_data.getSchema()));

		if (compression>0){
			data_writer.setCodec(CodecFactory.deflateCodec(compression));
		}
		data_writer.create(node_data.getSchema(), new File(path));		
		Iterator<NodeAvro> it = nodes.iterator();
		while (it.hasNext()) {
			node_data = it.next().getData();
			data_writer.append(node_data);
		}

		data_writer.close();
	}

	private void print_output(long start_time, long end_time, long read_start, long read_end, String format, String path){
		// TODO(jlewi): Compute File Size
		double nseconds = (end_time-start_time)/1000.0;

		double read_seconds =(read_end-read_start)/1000.0;
		StringBuilder sb = new StringBuilder();

		File file_handle = new File(path);
		double size = file_handle.length()/1e6;

		// Send all output to the Appendable object sb
		Formatter formatter = new Formatter(sb, Locale.US);
		formatter.format("Format:%s \t Write Time(sec): %f \t Read Time(sec): %f \t Output Size (Mb):%f", format,nseconds,read_seconds,size);
		formatter.flush();
		System.out.println(sb.toString());

	}

	/**
	 * Time how long it takes to write and read the nodes.
	 * @param path
	 * @param compression
	 */
	private void write_read_avro(ArrayList<NodeAvro> nodes, String avro_file,int compression) throws java.io.IOException{
		long start_time;
		long end_time;
		long read_start;
		long read_end;
		double nseconds;

		// TODO(jlewi): get a temporary file name				
		start_time=System.currentTimeMillis();
		WriteAvroFile(avro_file,nodes,compression);
		end_time=System.currentTimeMillis();

		//read the file
		read_start=System.currentTimeMillis();
		ReadAvroFile(avro_file);
		read_end=System.currentTimeMillis();		
		print_output(start_time,end_time,read_start,read_end,"avro ("+compression+")",avro_file);
	}
	
	/**
	 * Time how long it takes to write and read the nodes.
	 * @param path
	 * @param compression
	 */
	private void write_read_text(ArrayList<Node> nodes, String avro_file) throws java.io.IOException{
		long start_time;
		long end_time;
		long read_start;
		long read_end;
		double nseconds;

		// TODO(jlewi): get a temporary file name				
		start_time=System.currentTimeMillis();
		WriteTextFile(avro_file,nodes);
		end_time=System.currentTimeMillis();

		//read the file
		read_start=System.currentTimeMillis();
		ReadTextFileToTime(avro_file);
		read_end=System.currentTimeMillis();		
		print_output(start_time,end_time,read_start,read_end,"custom",avro_file);
	}
	// First argument should be a filename containing data in some default format.
	// This comprises the test data. We will read it in and construct an array of
	// Nodes. 
	public int run(String[] args) throws Exception 
	{

		// get stored in CommandLine.args which get passed onto ContrailConfig.parseOptions.		
		Option input = new Option("input","input",true,"The hadoop sequence file containging the output BuildGraph.");

		Options options = new Options();
		options.addOption(input);
		CommandLineParser parser = new GnuParser();
		CommandLine line = parser.parse( options, args, true );
		
		if (!line.hasOption("input")){
			System.out.println("ERROR: Missing required arguments");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "TimeSerialize", options );
			return -1;
		}
		// File containing the data to parse.
		String inputPath = line.getOptionValue("input");

		ArrayList<Node> txt_nodes = ReadTextFile(inputPath);
		ArrayList<NodeAvro> avro_nodes = new ArrayList<NodeAvro>();
		ArrayList<NodeAvro> read_avro;
		ArrayList<Node> read_txt;
		// convert the nodes to NodeAvro type
		for (Iterator<Node> it=txt_nodes.iterator(); it.hasNext();){
			NodeBase base = it.next();			
			avro_nodes.add(new NodeAvro(base));
		}

		// time how long it takes to serialize the nodes
		// TODO(jlewi): get a temporary file name
		String tmp_text="/tmp/text_nodes";
		String node_msg;
		long start_time;
		long end_time;
		long read_start;
		long read_end;
		double nseconds;
		int compression = 0;
		// time serialization using avro
		// TODO(jlewi): get a temporary file name
		String avro_file="/tmp/avro_nodes";		
		write_read_avro(avro_nodes,avro_file,0);
		write_read_avro(avro_nodes,avro_file,0);
		write_read_avro(avro_nodes,avro_file,4);
		

		write_read_text(txt_nodes,tmp_text);

		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////	
	public static void main(String[] args) throws Exception 
	{
		
		TimeSerialize timer= new TimeSerialize();
		int res = timer.run(args);
		System.exit(res);
	}
}
