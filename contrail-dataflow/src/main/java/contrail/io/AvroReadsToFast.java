/*
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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.correct.CorrectUtil;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * Convert avro files containing reads to fasta/fastq files.
 */
public class AvroReadsToFast extends MRStage {
  private static final Logger sLogger = Logger.getLogger(
      AvroReadsToFast.class);

  public static class ToFastMapper extends MapReduceBase
      implements Mapper<AvroWrapper<Object>, NullWritable,
                        Text, NullWritable> {
    private Text outText;
    private CorrectUtil correctUtil;

    @Override
    public void configure(JobConf job) {
      outText = new Text();
      correctUtil = new CorrectUtil();
    }

    @Override
    public void map(AvroWrapper<Object> key, NullWritable value,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
            throws IOException {
      if (key.datum() instanceof Read) {
        Read read = (Read) key.datum();
        if (read.getFastq() != null) {
          FastQRecord fastq = read.getFastq();
          outText.set(correctUtil.fastqRecordToString(fastq));
          collector.collect(outText, NullWritable.get());
        }
      }

      if (key.datum() instanceof FastQRecord) {
        FastQRecord fastq = (FastQRecord) key.datum();
        outText.set(correctUtil.fastqRecordToString(fastq));
        collector.collect(outText, NullWritable.get());
      }
    }
  }

  /**
   * Get the options required by this stage.
   */
  @Override
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

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    ArrayList<Schema> inputSchemas = new ArrayList<Schema>();
    inputSchemas.add((new Read()).getSchema());
    inputSchemas.add((new FastQRecord()).getSchema());
    AvroJob.setInputSchema(conf, Schema.createUnion(inputSchemas));
    AvroInputFormat<Object> input_format = new AvroInputFormat<Object>();
    conf.setInputFormat(input_format.getClass());
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    conf.setNumReduceTasks(0);
    conf.setMapperClass(ToFastMapper.class);
    conf.setReducerClass(IdentityReducer.class);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AvroReadsToFast(), args);
    System.exit(res);
  }
}
