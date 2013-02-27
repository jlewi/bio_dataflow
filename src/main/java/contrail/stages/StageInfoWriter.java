/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

/**
 * Writer for information about which stages executed.
 *
 */
public class StageInfoWriter {
  private static final Logger sLogger = Logger.getLogger(StageInfoWriter.class);

  private String outputPath;
  private Configuration conf;

  public StageInfoWriter(Configuration conf, String outputPath) {
    this.conf = conf;
    this.outputPath = outputPath;
  }

  /**
   * Write the stageInfo
   */
  public void write(StageInfo info) {
    // TODO(jlewi): We should cleanup old stage files after writing
    // the new one. Or we could try appending json records to the same file.
    // When I tried appending, the method fs.append threw an exception.
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    Date date = new Date();
    String timestamp = formatter.format(date);

    Path outputDir = new Path(outputPath);
    Path outputFile = new Path(FilenameUtils.concat(
        outputPath, "stage_info." + timestamp + ".json"));
    try {
      FileSystem fs = outputDir.getFileSystem(conf);
      if (!fs.exists(outputDir)) {
        fs.mkdirs(outputDir);
      }
      FSDataOutputStream outStream = fs.create(outputFile);

      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      generator.setPrettyPrinter(new DefaultPrettyPrinter());
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(
          info.getSchema(), generator);
      SpecificDatumWriter<StageInfo> writer =
          new SpecificDatumWriter<StageInfo>(StageInfo.class);
      writer.write(info, encoder);
      // We need to flush it.
      encoder.flush();
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't create the output stream.", e);
      System.exit(-1);
    }
  }
}
