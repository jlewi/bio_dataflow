/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.scaffolding;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * Convert the bowtie output to avro records.
 *
 * This mapper only job reads the output of bowtie and converts it to avro
 * records.
 *
 * Bowtie output is described at:
 * http://bowtie-bio.sourceforge.net/manual.shtml#default-bowtie-output
 */
public class BowtieConverter extends Stage {
  private static final Logger sLogger = Logger.getLogger(BowtieConverter.class);

  /**
   * Get the options required by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Mapper.
   */
  public static class ConvertMapper extends MapReduceBase implements Mapper<
      LongWritable, Text, AvroWrapper<BowtieMapping>, NullWritable> {

    private AvroWrapper<BowtieMapping> outputWrapper;
    private BowtieMapping mapping;
    private BowtieParser parser;

    public void configure(JobConf job) {
      mapping = new BowtieMapping();
      outputWrapper = new AvroWrapper<BowtieMapping>();
      outputWrapper.datum(mapping);
      parser = new BowtieParser();
    }

    public void map(
        LongWritable lineid, Text inputText,
        OutputCollector<AvroWrapper<BowtieMapping>, NullWritable> output,
        Reporter reporter) throws IOException {
      String line = inputText.toString();
      parser.parse(line, mapping);
      output.collect(outputWrapper, NullWritable.get());
   }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }

    initializeJobConfiguration(conf);

    sLogger.info("mapred.map.tasks=" + conf.get("mapred.map.tasks", ""));
    conf.setJobName(this.getClass().getName());

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    conf.setInputFormat(TextInputFormat.class);
    conf.setMapOutputKeyClass(AvroWrapper.class);
    conf.setMapOutputValueClass(NullWritable.class);

    conf.setMapperClass(ConvertMapper.class);

    // This is a mapper only job.
    conf.setNumReduceTasks(0);

    // TODO(jlewi): use setoutput codec to set the compression codec.
    AvroJob.setOutputSchema(conf, new BowtieMapping().getSchema());

    if (stage_options.containsKey("writeconfig")) {
      writeJobConfig(conf);
    } else {
      //delete the output directory if it exists already
      FileSystem.get(conf).delete(new Path(outputPath), true);

      long start_time = System.currentTimeMillis();
      RunningJob job = JobClient.runJob(conf);
      long end_time = System.currentTimeMillis();
      double nseconds = (end_time - start_time) / 1000.0;
      sLogger.info("Job took: " + nseconds + " seconds");
      return job;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new BowtieConverter(), args);
    System.exit(res);
  }
}
