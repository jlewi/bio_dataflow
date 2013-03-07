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
// Author: Deepak Nettem (deepaknettem@gmail.com)
package contrail.correct;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * Converts an avro file containing FastQRecords into a regular fastq file.
 */
public class AvroToFastQ extends MRStage {
  final Logger sLogger = Logger.getLogger(AvroToFastQ.class);

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

  public static class AvroToFastQMapper extends MapReduceBase
      implements Mapper<AvroWrapper<FastQRecord>, NullWritable,
                        Text, NullWritable> {
    private Text outKey;

    public void configure(JobConf job) {
      outKey = new Text();
    }

    @Override
    public void map(AvroWrapper<FastQRecord> key, NullWritable value,
        OutputCollector<Text, NullWritable> output, Reporter reporter)
            throws IOException {


      outKey.set(FastUtil.fastQRecordToString(key.datum()));
      output.collect(outKey, NullWritable.get());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    AvroJob.setInputSchema(conf, new FastQRecord().getSchema());

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroInputFormat<FastQRecord> input_format =
        new AvroInputFormat<FastQRecord>();
    conf.setInputFormat(input_format.getClass());

    // The output is a text file.
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    // We need to set the comparator because AvroJob.setInputSchema will
    // set it automatically to a comparator for an Avro class which we don't
    // want. We could also change the code to use an AvroMapper.
    conf.setOutputKeyComparatorClass(Text.Comparator.class);
    conf.setMapperClass(AvroToFastQMapper.class);
    conf.setReducerClass(IdentityReducer.class);
  }


  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AvroToFastQ(), args);
    System.exit(res);
  }
}
