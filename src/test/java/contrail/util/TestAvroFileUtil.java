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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.scaffolding.Library;

public class TestAvroFileUtil {
  @Test
  public void testPrettyPrintJsonRecords() throws IOException {
    File temp = File.createTempFile("temp", ".json");
    temp.deleteOnExit();

    // Delete the file if it exists because we want to test the code works
    // when the file doesn't exist.
    if (temp.exists()) {
      temp.delete();
    }

    List<Library> expectedLibs = new ArrayList<Library>();
    Random generator = new Random();

    String libraryPath = temp.getAbsolutePath();

    for (int i = 0; i < 2; ++i) {
      Library lib = new Library();
      lib.setName(String.format("lib_%d", i));
      lib.setMaxSize(generator.nextInt());
      lib.setMinSize(generator.nextInt());
      lib.setFiles(new ArrayList<CharSequence>());
      for (int j = 0; j < 2; ++j) {
        lib.getFiles().add(String.format("/tmp/lib_%d/read_%d", i, j));
      }
      expectedLibs.add(lib);
    }
    AvroFileUtil.prettyPrintJsonArray(
        new Configuration(), new Path(libraryPath), expectedLibs);
    List<Library> read = AvroFileUtil.readJsonArray(
        new Configuration(), new Path(libraryPath), Library.SCHEMA$);
    assertEquals(expectedLibs, read);
  }

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
