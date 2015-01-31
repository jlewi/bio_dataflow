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
// Author: Michael Schatz, Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.FileHelper;

public class TestGraphStats extends GraphStats {
  @Test
  public void testRun() {
    // Create a graph and write it to a file.
    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACTGGATT", 3);

    // Add some tips.
    builder.addEdge("ATT", "TTG", 2);
    builder.addEdge("ATT", "TTC", 2);

    File temp = FileHelper.createLocalTempDir();
    File avroFile = new File(temp, "graph.avro");

    GraphUtil.writeGraphToFile(avroFile, builder.getAllNodes().values());

    // Run it.
    GraphStats stage = new GraphStats();

    // We need to initialize the configuration otherwise we will get an
    // exception. Normally the initialization happens in main.
    stage.setConf(new Configuration());
    HashMap<String, Object> params = new HashMap<String, Object>();
    params.put("inputpath", avroFile.toString());

    File outputPath = new File(temp, "output");
    params.put("outputpath", outputPath.toString());

    stage.setParameters(params);

    // Catch the following after debugging.
    try {
      stage.execute();
    } catch (Exception exception) {
      exception.printStackTrace();
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
