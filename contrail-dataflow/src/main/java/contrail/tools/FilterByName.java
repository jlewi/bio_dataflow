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
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import contrail.graph.GraphNodeData;
import contrail.stages.ParameterDefinition;

/**
 * Select the nodes by name.
 */
public class FilterByName extends FilterBase {
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition filter = new ParameterDefinition(
        "nodes",
        "A comma separated list of the ids of the nodes to select. ",
        String.class,
        null);
    defs.put(filter.getName(), filter);
    return Collections.unmodifiableMap(defs);
  }

  public static class Filter extends AvroMapper<GraphNodeData, GraphNodeData> {
    HashSet<String> targets;

    @Override
    public void configure(JobConf job)    {
      FilterByName stage = new FilterByName();
      ParameterDefinition parameter =
          stage.getParameterDefinitions().get("nodes");
      String value = (String)parameter.parseJobConf(job);
      String[] pieces = value.split(",");
      targets = new HashSet<String>();
      for (String item : pieces) {
        targets.add(item);
      }
    }

    @Override
    public void map(
        GraphNodeData inData, AvroCollector<GraphNodeData> collector,
        Reporter reporter) throws IOException   {
      if (targets.contains(inData.getNodeId().toString())) {
        collector.collect(inData);
      }
    }
  }

  @Override
  public Class filterClass() {
    return Filter.class;
  }
}
