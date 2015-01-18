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

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

/**
 * Transforms for wrapping Avro mappers and reducers so they can be used with
 * Dataflow. Not all features supported by Avro Jobs are supported.
 * The reporter class for example currently does nothing.
 */
public class AvroMRTransforms {
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
    private JobConf jobConf;

    transient MAPPER mapper;
    transient DataflowAvroCollector collector;
    transient Reporter reporter;
    /**
     * A wrapper for AvroCollector that will emit the values using Dataflow.
     * @param <TYPE>
     */
    protected class DataflowAvroCollector extends
        AvroCollector<Pair<OUT_KEY, OUT_VALUE>> {
      public DoFn<I, KV<OUT_KEY, OUT_VALUE>>.ProcessContext c;

      @Override
      public void collect(Pair<OUT_KEY, OUT_VALUE> p) {
        c.output(KV.of(p.key(), p.value()));
      }
    }

    /**
     *
     * @param avroMapperClass The class for the mapper.
     */
    public AvroMapperDoFn(Class avroMapperClass, JobConf conf) {
      this.avroMapperClass = avroMapperClass;
      this.jobConf = conf;
      this.collector = new DataflowAvroCollector();
      this.reporter = new Reporter();
    }

    @Override
    public void startBundle(DoFn.Context c) {
      try {
        mapper = (MAPPER) avroMapperClass.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
      mapper.configure(jobConf);
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
    private JobConf jobConf;

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
        c.output(v);
      }
    }

    /**
     *
     * @param avroMapperClass The class for the mapper.
     */
    public AvroReducerDoFn(Class avroReducerClass, JobConf conf) {
      this.avroReducerClass = avroReducerClass;
      this.jobConf = conf;
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
}
