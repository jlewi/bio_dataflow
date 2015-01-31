/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.tools;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import contrail.graph.GraphUtil;
import contrail.graph.SimpleGraphBuilder;
import contrail.util.FileHelper;

public class TestPrettyPrintGraph {
  @Test
  public void testPrettyPrintGraph() {
    // This test just verifies we can write to stdout and not throw an error.
    // Test that the match works when we provide a directory and a glob path.
    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();
    Configuration conf = new Configuration();

    String inputPath = FilenameUtils.concat(tempDir, "input");

    SimpleGraphBuilder builder = new SimpleGraphBuilder();
    builder.addKMersForString("ACCTCG", 3);
    GraphUtil.writeGraphToFile(
        new File(inputPath), builder.getAllNodes().values());

    PrettyPrintGraph stage = new PrettyPrintGraph();
    stage.setParameter("inputpath", inputPath);
    assertTrue(stage.execute());
  }
}
