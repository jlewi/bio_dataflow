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
package contrail.util;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;

public class TestAvroFileUtil {
  @Test
  public void testReadFileSchema() {
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACCTCCG", 3);
    File tempDir = FileHelper.createLocalTempDir();
    Path avroPath = new Path(
        FilenameUtils.concat(tempDir.getPath(), "graph.avro"));

    GraphUtil.writeGraphToPath(
        new Configuration(), avroPath, builder.getAllNodes().values());

    Schema schema =
        AvroFileUtil.readFileSchema(new Configuration(), avroPath.toString());

    Schema expectedSchema = (new GraphNodeData()).getSchema();
    assertEquals(expectedSchema, schema);
  }
}
