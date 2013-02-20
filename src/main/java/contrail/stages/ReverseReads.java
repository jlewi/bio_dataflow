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
package contrail.stages;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
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

import contrail.io.FastQInputFormat;
import contrail.io.FastQWritable;
import contrail.sequences.DNAAlphabetWithNFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * Mapper only job to reverse the reads in a FastQ file.
 *
 * TODO(jeremy@lewi.us): This probably doesn't belong in package scaffolding.
 */
public class ReverseReads extends MRStage {
  private static final Logger sLogger = Logger.getLogger(ReverseReads.class);

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

  public static class ReverseMapper extends MapReduceBase
      implements Mapper<LongWritable, FastQWritable,
                        FastQWritable, NullWritable>{
    private Sequence sequence;
    private String qValue;

    public void configure(JobConf job) {
      sequence = new Sequence(DNAAlphabetWithNFactory.create());
    }

    public void map(LongWritable line, FastQWritable record,
        OutputCollector<FastQWritable, NullWritable> collector,
        Reporter reporter)
            throws IOException {
      // Get the reverse complement of the sequence.
      sequence.readCharSequence(record.getDNA());
      sequence = DNAUtil.reverseComplement(sequence);

      // Reverse the qValue
      qValue = record.getQValue();
      qValue = StringUtils.reverse(qValue);

      record.setDNA(sequence.toString());
      record.setQValue(qValue);
      collector.collect(record, NullWritable.get());
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    // Input
    conf.setMapperClass(ReverseMapper.class);
    conf.setInputFormat(FastQInputFormat.class);

    //Map Only Job
    conf.setNumReduceTasks(0);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new ReverseReads(), args);
    System.exit(res);
  }
}
