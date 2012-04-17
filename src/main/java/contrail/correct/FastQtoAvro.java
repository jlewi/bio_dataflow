package contrail.correct;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroJob; 
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

/*
 * This is a simple class used to convert Normal Fastq Data into the Avro format fastq data
 */

public class FastQtoAvro 
{       
  	
  public static class FastqPreprocessorMapper extends MapReduceBase  implements Mapper<LongWritable, Text, AvroWrapper<fastqrecord>, NullWritable> 
  {
    private int idx = 0;

    private String name = null;
    private String fqid = null;
    private String fqread = null; 
    private String filename = null;
    private String fqqvalue = null;


    private fastqrecord read = new fastqrecord();
    private AvroWrapper<fastqrecord> out_wrapper = new AvroWrapper<fastqrecord>(read);

    public void configure(JobConf job) 
    {
     
    }
/*
 * The mapper takes in key value pairs in the normal format and emits out Avro data
 */
    public void map(LongWritable lineid, Text line, OutputCollector<AvroWrapper<fastqrecord>, NullWritable> output, Reporter reporter) throws IOException 
    {

      if (idx == 0) 
      { 
    	  fqid = line.toString(); // The ID
      }
      else if (idx == 1) {    
    	
    	  fqread = line.toString(); // The kmer read
      }
      else if (idx == 2) { 
      }
      else if (idx == 3)
      {               
    	 fqqvalue = line.toString();
    	 /* Index of / is calculated since we need to make ids of mate pairs the same. We truncate everything
    	  * after this /. Error condition is not handled till now.
    	  */
    	 int ind = fqid.lastIndexOf('/');
     	 if (ind==-1){}//fastq not in proper format for mate pair reading
     	 fqid = fqid.substring(0,ind);
    	 read.id = fqid;
    	 read.qvalue = fqqvalue;
    	 read.read = fqread;
    	                                             
        output.collect(out_wrapper, NullWritable.get());

      }

      idx = (idx + 1) % 4;
            }

    public void close() throws IOException
    {
      if (idx != 0)
      {
    	  // Non multiple of 4  split
        throw new IOException("ERROR: closing with idx = " + idx + " in " + filename);
      }
    }

	
  }


  // Run Tool
  ///////////////////////////////////////////////////////////////////////////           
  public RunningJob run(String inputPath, String outputPath) throws Exception
  { 
	fastqrecord read = new fastqrecord();
	Schema OUT_SCHEMA = read.getSchema();

    JobConf conf = new JobConf(FastQtoAvro.class);
    conf.setJobName("Conversion from text to Avro" + inputPath);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setMapperClass(FastqPreprocessorMapper.class);
    conf.setNumReduceTasks(0);

    conf.setInputFormat(NLineInputFormat.class);
    conf.setInt("mapred.line.input.format.linespermap", 2000000); // must be a multiple of 4

    AvroJob.setOutputSchema(conf,OUT_SCHEMA);

    //delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }

    
    long start_time = System.currentTimeMillis();    
    RunningJob result = JobClient.runJob(conf);
    long end_time = System.currentTimeMillis();    
    double nseconds = (end_time - start_time) / 1000.0;
    System.out.println("Job took: " + nseconds + " seconds");
    
    return result;
  }


  
}