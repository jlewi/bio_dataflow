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

/**
 * A list of mapreduce counters used to communicate between jobs.
 *
 * We use several mapreduce counters to control processing; e.g. CompressChains
 * uses counters in CompressibleAvro to determine how many nodes can be
 * compressed and whether compression is done. This class defines the names
 * of different counters so we can be consistent across different jobs.
 *
 * TODO(jlewi): We should probably declare these as public variables inside
 *   the appropriate stages.
 */
public class GraphCounters {
  public static class CounterName {
    public CounterName (String group_name, String tag_name) {
      group = group_name;
      tag = tag_name;
    }
    public final String group;
    public final String tag;
  }

  public static CounterName quick_mark_nodes_send_to_compressor =
      new CounterName("Contrail", "nodes_to_send_to_compressor");
}
