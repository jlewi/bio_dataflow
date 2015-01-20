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
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.dataflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Transforms for wrapping Avro mappers and reducers so they can be used with
 * Dataflow. Not all features supported by Avro Jobs are supported.
 * The reporter class for example currently does nothing.
 */
public class AvroMRTransforms {
  private static final Logger sLogger = Logger.getLogger(
      AvroMRTransforms.class);
  private static class Reporter implements org.apache.hadoop.mapred.Reporter {
    @Override
    public void progress() {
      // TODO Auto-generated method stub
    }

    @Override
    public Counter getCounter(Enum<?> arg0) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Counter getCounter(String arg0, String arg1) {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public float getProgress() {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void incrCounter(Enum<?> arg0, long arg1) {
      // TODO Auto-generated method stub

    }

    @Override
    public void incrCounter(String arg0, String arg1, long arg2) {
      // TODO Auto-generated method stub

    }

    @Override
    public void setStatus(String arg0) {
      // TODO Auto-generated method stub
    }
  }

  // TODO(jeremy@lewi.us): This DoFn assumes the output of the AvroMapper
  // is a pair. However for map only jobs AvroMapper might not output a Pair.
  // What should we do in the case when the output isn't a pair?
  public static class AvroMapperDoFn<
      MAPPER extends AvroMapper<I, Pair<OUT_KEY, OUT_VALUE>>, I, OUT_KEY, OUT_VALUE>
          extends DoFn<I, KV<OUT_KEY, OUT_VALUE>> {
    private Class avroMapperClass;
    private byte[] jobConfBytes;
    private String keySchemaJson;
    private String valueSchemaJson;

    transient private Schema keySchema;
    transient private Schema valueSchema;
    transient MAPPER mapper;
    transient DataflowAvroCollector collector;
    transient Reporter reporter;
    transient JobConf jobConf;
    /**
     * A wrapper for AvroCollector that will emit the values using Dataflow.
     * @param <TYPE>
     */
    protected class DataflowAvroCollector extends
        AvroCollector<Pair<OUT_KEY, OUT_VALUE>> {
      public DoFn<I, KV<OUT_KEY, OUT_VALUE>>.ProcessContext c;

      @Override
      public void collect(Pair<OUT_KEY, OUT_VALUE> p) {
        // We need to make a copy of the data because the mapper could
        // reuse the variables.
        OUT_KEY k = SpecificData.get().deepCopy(keySchema, p.key());
        OUT_VALUE v = SpecificData.get().deepCopy(valueSchema, p.value());
        c.output(KV.of(k, v));
      }
    }

    /**
     *
     * @param avroMapperClass The class for the mapper.
     */
    public AvroMapperDoFn(
        Class avroMapperClass, JobConf conf, Schema outKeySchema,
        Schema outValueSchema) {
      this.avroMapperClass = avroMapperClass;
      this.jobConf = conf;
      this.keySchema = outKeySchema;
      this.valueSchema = outValueSchema;

      this.keySchemaJson = keySchema.toString();
      this.valueSchemaJson = valueSchema.toString();

      jobConfBytes = serializeJobConf(conf);
      collector = new DataflowAvroCollector();
      reporter = new Reporter();
    }

    @Override
    public void startBundle(DoFn.Context c) {
      try {
        mapper = (MAPPER) avroMapperClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      this.keySchema = Schema.parse(keySchemaJson);
      this.valueSchema = Schema.parse(valueSchemaJson);
      jobConf = deserializeJobConf(jobConfBytes);
      collector = new DataflowAvroCollector();
      mapper.configure(jobConf);
      reporter = new Reporter();
    }

    @Override
    public void processElement(
        DoFn<I, KV<OUT_KEY, OUT_VALUE>>.ProcessContext c) throws Exception {
      collector.c = c;
      I input = c.element();
      mapper.map(input, collector, reporter);
    }
  }


  public static class AvroReducerDoFn<
    REDUCER extends AvroReducer<INPUT_KEY, INPUT_VALUE, OUT>, INPUT_KEY, INPUT_VALUE, OUT>
      extends DoFn<KV<INPUT_KEY, Iterable<INPUT_VALUE>>, OUT> {

    private Class avroReducerClass;
    private byte[] jobConfBytes;
    private String valueSchemaJson;

    transient Schema valueSchema;
    transient private JobConf jobConf;
    transient REDUCER reducer;
    transient DataflowAvroCollector collector;
    transient Reporter reporter;

    /**
     * A wrapper for AvroCollector that will emit the values using Dataflow.
     * @param <TYPE>
     */
    protected class DataflowAvroCollector extends
        AvroCollector<OUT> {
      public DoFn<KV<INPUT_KEY, Iterable<INPUT_VALUE>>, OUT>.ProcessContext c;

      @Override
      public void collect(OUT v) {
        // We need to make a copy of the data because the reuse could
        // reuse the variables.
        OUT copy = SpecificData.get().deepCopy(valueSchema, v);
        c.output(copy);
      }
    }

    /**
     *
     * @param avroMapperClass The class for the mapper.
     */
    public AvroReducerDoFn(Class avroReducerClass, JobConf conf,
        Schema valueSchema) {
      this.avroReducerClass = avroReducerClass;
      this.valueSchema = valueSchema;
      this.valueSchemaJson = this.valueSchema.toString();
      this.jobConf = conf;
      this.jobConfBytes = serializeJobConf(conf);
      this.collector = new DataflowAvroCollector();
      this.reporter = new Reporter();
    }

    @Override
    public void startBundle(DoFn.Context c) {
      try {
        reducer = (REDUCER) avroReducerClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      valueSchema = Schema.parse(valueSchemaJson);
      jobConf = deserializeJobConf(jobConfBytes);
      collector = new DataflowAvroCollector();
      reporter = new Reporter();
      reducer.configure(jobConf);
    }

    @Override
    public void processElement(
        DoFn<KV<INPUT_KEY, Iterable<INPUT_VALUE>>, OUT>.ProcessContext c)
        throws Exception {
      collector.c = c;
      KV<INPUT_KEY, Iterable<INPUT_VALUE>> inputs = c.element();
      reducer.reduce(
          inputs.getKey(), inputs.getValue(), this.collector,
          this.reporter);
    }
  }

  /**
   * Serialize the job configuration to an array of byte.
   * @param conf
   * @return
   */
  public static byte[] serializeJobConf(JobConf conf) {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(byteStream);
    try {
      conf.write(dataStream);
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem serializing job conf.", e);
    }
    return byteStream.toByteArray();
  }

  /**
   * Serialize the job configuration to an array of byte.
   * @param conf
   * @return
   */
  public static JobConf deserializeJobConf(byte[] bytes) {
    JobConf jobConf = new JobConf();

    ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
    DataInputStream dataStream = new DataInputStream(byteStream);
    try {
      jobConf.readFields(dataStream);
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem deserializing job conf.", e);
    }
    return jobConf;
  }
}
