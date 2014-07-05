/* Licensed under the Apache License, Version 2.0 (the "License");
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.correct.FastQToAvro;
import contrail.io.mapred.FastQInputFormat;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * Convert FastQ files to avro files containing Read records.
 *
 */
public class FastQToRead extends MRStage {
  final Logger sLogger = Logger.getLogger(FastQToRead.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String,
        ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def :
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(defs);
  }

  public static class FastQToReadMapper extends MapReduceBase
      implements Mapper<LongWritable, FastQWritable,
                        AvroWrapper<Read>, NullWritable> {
    private final Read read = new Read();
    private final FastQRecord fastq = new FastQRecord();
    private final AvroWrapper<Read> outWrapper = new AvroWrapper<Read>(read);

    @Override
    public void map(LongWritable line, FastQWritable record,
        OutputCollector<AvroWrapper<Read>, NullWritable> output, Reporter reporter)
            throws IOException {
      fastq.setId(record.getId());
      fastq.setRead(record.getDNA());
      fastq.setQvalue(record.getQValue());

      read.setFastq(fastq);
      output.collect(outWrapper, NullWritable.get());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    conf.setJobName("FastQToRead");

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    AvroJob.setOutputSchema(conf,new FastQRecord().getSchema());

    conf.setMapperClass(FastQToReadMapper.class);
    conf.setInputFormat(FastQInputFormat.class);

    // Map Only Job
    conf.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FastQToAvro(), args);
    System.exit(res);
  }
}

