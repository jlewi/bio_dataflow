package contrail.correct;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.FastQRecord;
import contrail.stages.BuildGraphAvro;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;
import java.util.*;
import contrail.correct.FastQText;
import contrail.correct.FastQInputFormat;


/** MapReduce job to convert a FastQ File to Avro. 
 * Uses FastQInputFormat.
 * @author dnettem
 */
public class FastQToAvro extends Stage {

/*  private static*/ final Logger sLogger = Logger.getLogger(BuildGraphAvro.class);

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, 
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def : ContrailParameters
        .getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public static class FastQToAvroMapper extends MapReduceBase 
  implements Mapper<LongWritable, FastQText, AvroWrapper<FastQRecord>, NullWritable>{

    private FastQRecord read = new FastQRecord();
    private AvroWrapper<FastQRecord> out_wrapper = new AvroWrapper<FastQRecord>(read);
    
    public void configure(JobConf job)
    {
      Configuration conf = new Configuration();
      FileSystem hdfs;
    }
    
    public void map(LongWritable line, FastQText record,
        OutputCollector<AvroWrapper<FastQRecord>, NullWritable> output, Reporter reporter)
            throws IOException {
      
      read.id = record.getId(); 
      read.read = record.getDna();
      read.qvalue = record.getQValue();
      
      output.collect(out_wrapper, NullWritable.get());     
    }
  }

  public RunningJob runJob() throws Exception {
    
    JobConf conf = new JobConf(FastQTestMR.class);
    
    conf.setJobName("FastQToAvro");
    String inputPath, outputPath, datatype;
    Long splitSize;
    conf.setJobName("Rekey Data");
    String[] required_args = {"inputpath", "outputpath","splitSize"};
    checkHasParametersOrDie(required_args);   
    
    inputPath = (String) stage_options.get("inputpath");
    outputPath = (String) stage_options.get("outputpath");
    splitSize = (Long) stage_options.get("splitSize");
    
    //Sets the parameters in JobConf
    initializeJobConfiguration(conf);
    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
   
    // Input
    conf.setMapperClass(FastQToAvroMapper.class);
    conf.setInputFormat(FastQInputFormat.class);
    conf.setLong("FastQInputFormat.splitSize", splitSize);
    //Map Only Job
    conf.setNumReduceTasks(0);
    
    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);  
    }     
    
    long starttime = System.currentTimeMillis();            
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) (((float) (endtime - starttime)) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return result;   
  }
 
  public static void main(String[] args) throws Exception {
    
    int res = ToolRunner.run(new Configuration(), new FastQTestMR(), args);
    System.exit(res);
  }
}

