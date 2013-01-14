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

    ParameterDefinition subLen =
        new ParameterDefinition(
            "sub_length", "Length for the alignment.", Integer.class, null);

    defs.put(subLen.getName(), subLen);

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

    private int subLen;
    private AvroWrapper<BowtieMapping> outputWrapper;
    private BowtieMapping mapping;

    public void configure(JobConf job) {
      mapping = new BowtieMapping();
      outputWrapper = new AvroWrapper<BowtieMapping>();
      outputWrapper.datum(mapping);

      BowtieConverter stage = new BowtieConverter();
      Map<String, ParameterDefinition> definitions =
          stage.getParameterDefinitions();
      subLen =  (Integer) (definitions.get("sub_length").parseJobConf(job));
    }

    public void map(
        LongWritable lineid, Text inputText,
        OutputCollector<AvroWrapper<BowtieMapping>, NullWritable> output,
        Reporter reporter) throws IOException {
      String line = inputText.toString();
      // For the description of the bowtie output see:
      // http://bowtie-bio.sourceforge.net/manual.shtml#default-bowtie-output
      //
      // Split the string based on whitespace. "\s" matches a whitespace
      // character and the + modifier causes it to be matched one or more
      // times. An extra "\" is needed to escape the "\s".
      // For more info:
      // http://docs.oracle.com/javase/tutorial/essential/regex/
      String[] splitLine = line.trim().split("\\s+");

      // The line should have at least 5 fields.
      if (splitLine.length <5) {
        sLogger.fatal(
            "Line in the bowtie output file had less than 5 fields:" + line,
            new RuntimeException("Parse Error"));
        System.exit(-1);
      }

      // The first field in the output is the name of the read that was
      // aligned. The second field is a + or - indicating which strand
      // the read aligned to.
      String readID = splitLine[0];
      String strand = splitLine[1];
      String contigID = splitLine[2];
      // 0-based offset into the forward reference strand where leftmost
      // character of the alignment occurs.
      String forwardOffset = splitLine[3];
      String readSequence = splitLine[4];

      mapping.setContigId(contigID);

      // isFWD indicates the read was aligned to the forward strand of
      // the reference genome.
      Boolean isFwd = null;
      if (strand.equals("-")) {
        isFwd = false;
      } else if (strand.equals("+")) {
        isFwd = true;
      } else {
        throw new RuntimeException("Couldn't parse the alignment strand");
      }

      // TODO(jeremy@lewi.us): It looks like the original code prefixed
      // the read id's with the library name.
      // We don't prefix the read with the library
      // name because we think that's more likely to cause problems because
      // the read ids would need to be changed consistently everywhere.
      mapping.setReadId(readID);

      // TODO(jeremy@lewi.us): The original code was using 1 based indexing
      // for the start of the read
      mapping.setReadStart(1);
      // TODO(jeremy@lewi.us): Need to check whether the length should be
      // zero based or 1 based. The original code set this to SUB_LEN
      // which was the length of the truncated reads which were aligned.
      // TODO(jerem@lewi.us): Do we have to pass in SUB_LEN or can we
      // determine it from the output.
      mapping.setReadEnd(subLen);

      mapping.setContigStart(Integer.parseInt(forwardOffset));
      if (isFwd) {
        mapping.setContigEnd(
            mapping.getContigStart() + readSequence.length() - 1);
      } else {
        mapping.setContigEnd(mapping.getContigStart());
        mapping.setContigStart(
            mapping.getContigEnd() + readSequence.length() - 1);
      }
      output.collect(outputWrapper, NullWritable.get());
   }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath", "sub_length"};
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
      long numReads =
          job.getCounters().findCounter(
              "org.apache.hadoop.mapred.Task$Counter",
              "MAP_OUTPUT_RECORDS").getValue();
      sLogger.info("Number of reads:" + numReads);
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
