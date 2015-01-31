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
package contrail.tools;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import contrail.graph.GraphNodeData;
import contrail.stages.ParameterDefinition;

/**
 * Select all nodes whose length is greater than or equal to some length.
 *
 */
public class FilterByMinLength extends FilterBase {
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition filter = new ParameterDefinition(
        "min_length",
        "The minimum length for the nodes",
        Integer.class,
        null);
    defs.put(filter.getName(), filter);
    return Collections.unmodifiableMap(defs);
  }

  public static class Filter extends AvroMapper<GraphNodeData, GraphNodeData> {
    private int minValue;

    @Override
    public void configure(JobConf job)    {
      FilterByMinLength stage = new FilterByMinLength();
      ParameterDefinition parameter =
          stage.getParameterDefinitions().get("min_length");
      minValue = (Integer)parameter.parseJobConf(job);
    }

    @Override
    public void map(
        GraphNodeData inData, AvroCollector<GraphNodeData> collector,
        Reporter reporter) throws IOException   {
      if (inData.getSequence().getLength() >= minValue) {
        collector.collect(inData);
      }
    }
  }

  @Override
  public Class filterClass() {
    return Filter.class;
  }
}
