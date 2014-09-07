/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package contrail.dataflow.transforms;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertThat;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import contrail.dataflow.AvroSpecificCoder;
import contrail.dataflow.GCSAvroFileSplit;

public class TestEncodeAvroAsJson {
  @Test
  public void testEncodeAvroAsJson() {
    Pipeline p = Pipeline.create();

    // Create some avro records to use for testing.
    ArrayList<GCSAvroFileSplit> splits = new ArrayList<GCSAvroFileSplit>();
    for (String name : new String[]{"file1", "file2", "file3"}) {
      GCSAvroFileSplit split = new GCSAvroFileSplit();
      split.setPath(name.toString());
      splits.add(split);
    }

    PCollection<GCSAvroFileSplit> inputs = p.begin().apply(Create.of(splits))
        .setCoder(AvroSpecificCoder.of(GCSAvroFileSplit.class));

    PCollection<String> jsonRecords = inputs.apply(
        new EncodeAvroAsJson<GCSAvroFileSplit>(GCSAvroFileSplit.SCHEMA$));

    EvaluationResults results = p.run(DirectPipelineRunner.createForTest());
    List<String> jsonStrings = results.getPCollection(jsonRecords);

    assertThat(jsonStrings,
        containsInAnyOrder(
            "{\"path\":\"file1\"}",
            "{\"path\":\"file2\"}",
            "{\"path\":\"file3\"}"));
  }
}
