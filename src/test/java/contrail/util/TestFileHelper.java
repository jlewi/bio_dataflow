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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestFileHelper {
  private HashSet<String> pathToStringSet(Collection<Path> paths) {
    HashSet<String> set = new HashSet<String>();
    for (Path path : paths) {
      set.add(path.toUri().getPath());
    }
    return set;
  }

  @Test
  public void testMatchGlobWithDefaultDir() {
    // Test that the match works when we provide a directory and a glob path.
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();
    Configuration conf = new Configuration();

    for (String name : new String[]{"file", "file1.avro", "file2.avro"}) {
      Path path = new Path(FilenameUtils.concat(tempDir, name));
      List<String> nodeData = Arrays.asList("some text");
      AvroFileUtil.writeRecords(
          conf, path, nodeData, Schema.create(Type.STRING));
    }

    ArrayList<Path> matches = FileHelper.matchGlobWithDefault(
        conf, tempDir, "*.avro");

    HashSet<String> expected = new HashSet<String>();
    expected.add(FilenameUtils.concat(tempDir, "file1.avro"));
    expected.add(FilenameUtils.concat(tempDir, "file2.avro"));
    assertEquals(2, matches.size());
    assertEquals(expected, pathToStringSet(matches));
  }

  @Test
  public void testMatchGlobWithDefaultNoExist() {
    // Test that the match returns an empty list when we supply a directory
    // which doesn't exist.
    Configuration conf = new Configuration();

    ArrayList<Path> matches = FileHelper.matchGlobWithDefault(
        conf, "/some/nonexistent/directory/123adf234!@#", "*");

    assertTrue(matches.isEmpty());
  }

  @Test
  public void testMatchGlob() {
    // Test that the match works when we provide a glob path.
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();
    Configuration conf = new Configuration();

    for (String name : new String[]{"file", "file1.rnd", "file2.rnd"}) {
      Path path = new Path(FilenameUtils.concat(tempDir, name));
      List<String> nodeData = Arrays.asList("some text");
      AvroFileUtil.writeRecords(
          conf, path, nodeData, Schema.create(Type.STRING));
    }

    // The glob path and default glob should be different.
    ArrayList<Path> matches = FileHelper.matchGlobWithDefault(
        conf, FilenameUtils.concat(tempDir, "*.rnd"), "*.avro");

    HashSet<String> expected = new HashSet<String>();
    expected.add(FilenameUtils.concat(tempDir, "file1.rnd"));
    expected.add(FilenameUtils.concat(tempDir, "file2.rnd"));
    assertEquals(2, matches.size());
    assertEquals(expected, pathToStringSet(matches));
  }

  @Test
  public void testMatchGlobWithFileSystem() {
    // Test that the match works when we provide a glob path that includes
    // the filesystem.
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();
    Configuration conf = new Configuration();

    for (String name : new String[]{"file", "file1.rnd", "file2.rnd"}) {
      Path path = new Path(FilenameUtils.concat(tempDir, name));
      List<String> nodeData = Arrays.asList("some text");
      AvroFileUtil.writeRecords(
          conf, path, nodeData, Schema.create(Type.STRING));
    }

    // The glob path and default glob should be different.
    ArrayList<Path> matches = FileHelper.matchGlobWithDefault(
        conf, "file://" + FilenameUtils.concat(tempDir, "*.rnd"), "*.avro");

    HashSet<String> expected = new HashSet<String>();
    expected.add(FilenameUtils.concat(tempDir, "file1.rnd"));
    expected.add(FilenameUtils.concat(tempDir, "file2.rnd"));
    assertEquals(2, matches.size());
    assertEquals(expected, pathToStringSet(matches));
  }

  @Test
  public void testMatchListOfGlobs() {
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();
    Configuration conf = new Configuration();

    for (String name : new String[]{
         "somefile.avro", "file1.rnd", "file2.rnd", "pattern1.txt",
         "pattern2.txt", "nomatch.no"}) {
      Path path = new Path(FilenameUtils.concat(tempDir, name));
      List<String> nodeData = Arrays.asList("some text");
      AvroFileUtil.writeRecords(
          conf, path, nodeData, Schema.create(Type.STRING));
    }

    // The glob is a comma separated list of a specific file and globs.
    String globList = StringUtils.join(new String[]{
      FilenameUtils.concat(tempDir, "somefile.avro"),
      FilenameUtils.concat(tempDir, "*.rnd"),
      FilenameUtils.concat(tempDir, "*.txt")}, ",");

    ArrayList<Path> matches = FileHelper.matchListOfGlobsWithDefault(
        conf, globList, "*");

    HashSet<String> expected = new HashSet<String>();
    expected.add(FilenameUtils.concat(tempDir, "somefile.avro"));
    expected.add(FilenameUtils.concat(tempDir, "file1.rnd"));
    expected.add(FilenameUtils.concat(tempDir, "file2.rnd"));
    expected.add(FilenameUtils.concat(tempDir, "pattern1.txt"));
    expected.add(FilenameUtils.concat(tempDir, "pattern2.txt"));
    assertEquals(5, matches.size());
    assertEquals(expected, pathToStringSet(matches));
  }
}
