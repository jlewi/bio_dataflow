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
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.io.AvroFilesIterator;
import contrail.util.FileHelper;

public class TestAvroFilesIterator {
  @Test
  public void testIterator() {
    int numFiles = 2;
    ArrayList<Path> files = new ArrayList<Path>();

    File temp = FileHelper.createLocalTempDir();
    for (int index = 0; index < numFiles; ++index) {
      File avroFile = new File(temp, String.format("file_%d.avro", index));
      files.add(new Path(avroFile.getPath()));

      // Write the data to the file.
      Schema schema = Schema.create(Schema.Type.STRING);
      DatumWriter<CharSequence> datumWriter =
          new SpecificDatumWriter<CharSequence>(schema);
      DataFileWriter<CharSequence> writer =
          new DataFileWriter<CharSequence>(datumWriter);

      try {
        writer.create(schema, avroFile);
        writer.append("index_" + index);
        writer.close();
      } catch (IOException exception) {
        fail("There was a problem writing the data to an avro file. " +
             "Exception: " + exception.getMessage());
      }
    }

    AvroFilesIterator<CharSequence> iter =
        new AvroFilesIterator<CharSequence>(new Configuration(), files);

    HashSet<String> data = new HashSet<String>();
    while (iter.hasNext()) {
      data.add(iter.next().toString());
    }
    assertEquals(numFiles, data.size());
  }
}
