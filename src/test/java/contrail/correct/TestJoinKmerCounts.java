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
package contrail.correct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestJoinKmerCounts {
  private static class Counts {
    public Long before;
    public Long after;

    public Counts(Long b, Long a) {
      before = b;
      after = a;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Counts)) {
        return false;
      }
      Counts o = (Counts) other;
      if (!o.before.equals(before)) {
        return false;
      }
      if (!o.after.equals(after)) {
        return false;
      }
      return true;
    }
  }

  @Test
  public void testMR() {
    File tempDir = FileHelper.createLocalTempDir();
    String afterDir = FilenameUtils.concat(tempDir.getPath(), "after");
    String beforeDir = FilenameUtils.concat(tempDir.getPath(), "before");

    HashMap<String, Counts> data = new HashMap<String, Counts>();
    data.put("AAAFC", new Counts(10L, 13L));
    data.put("AAGC", new Counts(34L, 234L));

    ArrayList<Pair<CharSequence, Long>> beforeCounts =
        new ArrayList<Pair<CharSequence, Long>>();
    ArrayList<Pair<CharSequence, Long>> afterCounts =
        new ArrayList<Pair<CharSequence, Long>>();
    for (String key  : data.keySet()) {
      Pair<CharSequence, Long> before = new Pair<CharSequence, Long>(
          key, data.get(key).before);
      Pair<CharSequence, Long> after = new Pair<CharSequence, Long>(
          key, data.get(key).after);

      beforeCounts.add(before);
      afterCounts.add(after);
    }

    AvroFileUtil.writeRecords(
        new Configuration(),
        new Path(FilenameUtils.concat(beforeDir, "counts.avro")),
        beforeCounts);

    AvroFileUtil.writeRecords(
        new Configuration(),
        new Path(FilenameUtils.concat(afterDir, "counts.avro")),
        afterCounts);

    JoinKmerCounts stage = new JoinKmerCounts();
    stage.setParameter("before", beforeDir);
    stage.setParameter("after", afterDir);
    String outputPath = FilenameUtils.concat(tempDir.getPath(), "outputpath");
    stage.setParameter("outputpath", outputPath);

    assertTrue(stage.execute());

    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = mapper.getJsonFactory();

    HashMap<String, Counts> actual = new HashMap<String, Counts>();
    try {
      BufferedReader reader = new BufferedReader(new FileReader(
          FilenameUtils.concat(outputPath, "part-00000")));

      String line;
      while ((line = reader.readLine()) != null) {
        JsonParser jp = factory.createJsonParser(line);
        JsonNode node = mapper.readTree(jp);

        Counts counts = new Counts(
            node.getFieldValue("before").getLongValue(),
            node.getFieldValue("after").getLongValue());
        actual.put(node.getFieldValue("Kmer").getTextValue(), counts);
      }
      reader.close();
    } catch (IOException e) {
      fail(e.getMessage());
    }

    assertEquals(data, actual);
  }
}
