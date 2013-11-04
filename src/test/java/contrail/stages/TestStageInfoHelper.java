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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestStageInfoHelper {
  @Test
  public void testDFSSeach() {
    // Test DFS serach.
    StageInfo root = new StageInfo();
    root.setStageClass("root");
    root.setSubStages(new ArrayList<StageInfo>());

    StageInfo a = new StageInfo();
    a.setStageClass("a");
    a.setSubStages(new ArrayList<StageInfo>());
    root.getSubStages().add(a);

    StageInfo b = new StageInfo();
    b.setStageClass("b");
    b.setSubStages(new ArrayList<StageInfo>());
    root.getSubStages().add(b);

    StageInfo a1 = new StageInfo();
    a1.setStageClass("a1");
    a.getSubStages().add(a1);

    StageInfo a2 = new StageInfo();
    a2.setStageClass("a2");
    a.getSubStages().add(a2);

    StageInfo b1 = new StageInfo();
    b1.setStageClass("b1");
    b.getSubStages().add(b1);

    StageInfo b2 = new StageInfo();
    b2.setStageClass("b2");
    b.getSubStages().add(b2);

    ArrayList<String> expected = new ArrayList<String>();
    expected.addAll(Arrays.asList(
        "b2", "b1", "b", "a2", "a1", "a", "root"));

    ArrayList<String> actual = new ArrayList<String>();
    StageInfoHelper helper = new StageInfoHelper(root);
    for (StageInfo stage : helper.DFSSearch()) {
      actual.add(stage.getStageClass().toString());
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testLoadMostRecent() {
    StageInfo root = new StageInfo();
    root.setStageClass("root");
    root.setSubStages(new ArrayList<StageInfo>());
    root.setState(StageState.SUCCESS);
    root.setCounters(new ArrayList<CounterInfo>());
    root.setParameters(new ArrayList<StageParameter>());
    root.setModifiedParameters(new ArrayList<StageParameter>());

    File temp = FileHelper.createLocalTempDir();

    Configuration conf = new Configuration();
    ArrayList<StageInfo> records = new ArrayList<StageInfo>();
    records.add(root);

    ArrayList<Path> paths = new ArrayList<Path>();

    StageInfoWriter writer = new StageInfoWriter(conf, temp.getAbsolutePath());
    for (int i = 0; i < 3; ++i) {
      Path path = writer.write(root);
      paths.add(path);
    }

    String infoGlob = FilenameUtils.concat(temp.getAbsolutePath(), "*.json");
    StageInfoHelper helper = StageInfoHelper.loadMostRecent(
        conf, infoGlob) ;

    String expected = paths.get(paths.size() - 1).toUri().getPath();
    String actual = helper.getFilePath().toUri().getPath();
    assertEquals(expected, actual);
    assertEquals(root, helper.getInfo());
  }

  @Test
  public void testListDirectories() {
    // This test uses a json file that we pulled from one of our runs.
    String jsonResourcePath =
        "contrail/stages/stage_info.20131029-225613.json";

    InputStream inStream = this.getClass().getClassLoader().getResourceAsStream(
        jsonResourcePath);

    if (inStream == null) {
      fail("Could not find resource:" + jsonResourcePath);
    }

    Schema schema = (new StageInfo()).getSchema();
    ArrayList<StageInfo> records = AvroFileUtil.readJsonRecords(
        inStream, schema);
    if (records.size() != 1) {
      fail ("There should be exactly 1 record in the json file.");
    }

    StageInfoHelper helper = new StageInfoHelper(records.get(0));
    HashSet<String> paths = helper.listOutputPaths(new HashSet<String>());
    assertEquals(39, paths.size());

    // Mark all paths as deleted.
    helper.markPathsAsDeleted(paths);
    paths = helper.listOutputPaths(new HashSet<String>());
    assertEquals(0, paths.size());
  }
}
