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
package contrail.io;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestCollatingAvroFileContentsIterator {
  /**
   * Create a file with a random set of records in sorted order.
   *
   * @param numFiles
   * @param data
   * @return
   */
  protected List<Integer> writeSortedFile(String path, int size) {
    ArrayList<Integer> data = new ArrayList<Integer>(size);
    Random generator = new Random();
    for (int i = 0; i < size; ++i) {
      data.add(generator.nextInt(10000));
    }
    Collections.sort(data);

    Schema schema = Schema.create(Schema.Type.INT);
    AvroFileUtil.writeRecords(
        new Configuration(), new Path(path), data, schema);

    return data;
  }

  private static class SomeComparator implements Comparator<Integer> {
    @Override
    public int compare(Integer o1, Integer o2) {
      return o1.compareTo(o2);
    }
  }

  @Test
  public void testItertor() {
    Random generator = new Random();

    int numFiles = 10;
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();

    ArrayList<String> files = new ArrayList<String>();

    ArrayList<Integer> expected = new ArrayList<Integer>();
    for (int i = 0; i < numFiles; i++) {
      String path = FilenameUtils.concat(
          tempDir, String.format("data-%02d.avro", i));
      files.add(path);
      List<Integer> fileData = writeSortedFile(path, generator.nextInt(100));
      expected.addAll(fileData);
    }

    CollatingAvroFileContentsIterator<Integer> iterator = new
        CollatingAvroFileContentsIterator<Integer>(
            files, new Configuration(), new SomeComparator());

    ArrayList<Integer> actual = new ArrayList<Integer>();
    while (iterator.hasNext()) {
      actual.add(iterator.next());
    }

    Collections.sort(expected);
    assertEquals(expected, actual);
  }
}
