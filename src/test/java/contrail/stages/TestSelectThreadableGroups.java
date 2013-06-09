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
package contrail.stages;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.util.AvroFileContentsIterator;
import contrail.util.AvroFileUtil;
import contrail.util.CharUtil;
import contrail.util.FileHelper;

public class TestSelectThreadableGroups extends SelectThreadableGroups{
  @Test
  public void testJob() {
    // Create some groups.
    ArrayList<List<CharSequence>> groups = new ArrayList<List<CharSequence>>();

    List<CharSequence> l1 = new ArrayList<CharSequence>();
    l1.addAll(Arrays.asList("a", "b", "c"));
    groups.add(l1);

    List<CharSequence> l2 = new ArrayList<CharSequence>();
    l2.addAll(Arrays.asList("c", "d", "e"));
    groups.add(l2);

    List<CharSequence> l3 = new ArrayList<CharSequence>();
    l3.addAll(Arrays.asList("x", "y", "z"));
    groups.add(l3);

    File temp = FileHelper.createLocalTempDir();
    String groupsPath = FilenameUtils.concat(
        temp.getAbsolutePath(), "groups.avro");

    AvroFileUtil.writeRecords(
        new Configuration(), new Path(groupsPath), groups,
        Schema.createArray(Schema.create(Schema.Type.STRING)));

    SelectThreadableGroups stage = new SelectThreadableGroups();
    stage.setParameter("inputpath", groupsPath);
    stage.setParameter(
        "outputpath", FilenameUtils.concat(
            temp.getAbsolutePath(), "outputpath"));

    assertTrue(stage.execute());

    // Read the output.
    AvroFileContentsIterator<Pair<CharSequence, List<CharSequence>>> outputs
      = AvroFileContentsIterator.fromGlob(
          new Configuration(), stage.getOutPath().toString());

    List<String> merged = new ArrayList<String>();
    merged.addAll(CharUtil.toStringList(l1));
    merged.add("d");
    merged.add("e");

    Pair<CharSequence, List<CharSequence>> outPair = outputs.next();
    assertEquals("000", outPair.key().toString());
    assertEquals(merged, CharUtil.toStringList(outPair.value()));

    outPair = outputs.next();
    assertEquals("001", outPair.key().toString());
    assertEquals(
        CharUtil.toStringList(l3),
        CharUtil.toStringList(outPair.value()));

    assertFalse(outputs.hasNext());
  }

  @Test
  public void testNoMerge() {
    // In this case we test that when a merge can't be performed because
    // the result would be too large that the nodes don't get assigned to
    // any group.
    ArrayList<List<CharSequence>> groups = new ArrayList<List<CharSequence>>();

    List<CharSequence> l1 = new ArrayList<CharSequence>();
    l1.addAll(Arrays.asList("a", "b", "c"));
    groups.add(l1);

    List<CharSequence> l2 = new ArrayList<CharSequence>();
    l2.addAll(Arrays.asList("c", "d", "e"));
    groups.add(l2);

    this.setParameter("max_subgraph_size", 3);

    List<List<CharSequence>> threadableGroups =
        new ArrayList<List<CharSequence>> ();

    threadableGroups.add(l1);
    threadableGroups.add(l2);

    Collection<List<String>> subGraphs = this.selectSubGraphs(threadableGroups);

    assertEquals(1, subGraphs.size());

    assertEquals(CharUtil.toStringList(l1), subGraphs.iterator().next());
  }
}
