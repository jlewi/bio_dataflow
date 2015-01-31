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
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.tools;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BigQueryJsonEncoder;
import org.apache.avro.io.DatumWriter;
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
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.AvroFileUtil;
import contrail.util.BigQuerySchema;
import contrail.util.FileHelper;

/**
 * Convert avro records to Json suitable for importing into bigquery.
 *
 * Currently this only works if the avro records don't have nested records.
 *
 * The schema also needs to be the same for all the records.
 */
public class AvroToBigQueryJson extends MRStage {
  private static final Logger sLogger = Logger.getLogger(AvroToBigQueryJson.class);

  private Schema schema;


  public static class ToJsonMapper extends MapReduceBase
    implements Mapper<AvroWrapper<Object>, NullWritable,
                      Text, NullWritable> {

    private Text outText;
    private Schema schema;
    private ByteArrayOutputStream outStream;

    private JsonGenerator generator;
    private BigQueryJsonEncoder encoder;
    private DatumWriter<Object> writer;
    @Override
    public void configure(JobConf job) {
      String filename = job.get("map.input.file");
      schema = AvroFileUtil.readFileSchema(job, filename);
      outText = new Text();
      outStream = new ByteArrayOutputStream();

      writer = new GenericDatumWriter<Object>(schema);
      JsonFactory factory = new JsonFactory();
      try {
        generator = factory.createJsonGenerator(outStream);
        encoder = new BigQueryJsonEncoder(schema, generator);
      } catch (IOException e) {
        sLogger.fatal("Problem initializing the mapper.", e);
      }
    }

    @Override
    public void map(AvroWrapper<Object> key, NullWritable value,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
        throws IOException {
      outStream.reset();

      writer.write(key.datum(), encoder);
      encoder.flush();

      outStream.flush();
      outStream.close();

      outText.set(outStream.toString());
      collector.collect(outText, NullWritable.get());
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

    sLogger.info(" - inputpath: "  + inputPath);
    sLogger.info(" - outputpath: " + outputPath);

    ArrayList<Path> inputFiles = FileHelper.matchGlobWithDefault(
        getConf(), inputPath, "*.avro");

    if (inputFiles.size() == 0) {
      sLogger.fatal(
          "No files matched inputpath:" + inputPath,
          new RuntimeException("No input found."));
    }
    schema = AvroFileUtil.readFileSchema(
        getConf(), inputFiles.get(0).toString());
    AvroJob.setInputSchema(conf, schema);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroInputFormat<Object> input_format = new AvroInputFormat<Object>();
    conf.setInputFormat(input_format.getClass());
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    conf.setNumReduceTasks(0);
    conf.setMapperClass(ToJsonMapper.class);
    conf.setReducerClass(IdentityReducer.class);
  }

  @Override
  protected void postRunHook() {
    BigQuerySchema bigQuerySchema = BigQuerySchema.fromAvroSchema(schema) ;
    sLogger.info("Schema:\n" + bigQuerySchema.toString());
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AvroToBigQueryJson(), args);
    System.exit(res);
  }
}
