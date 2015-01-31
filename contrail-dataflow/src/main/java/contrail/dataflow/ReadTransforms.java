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
package contrail.dataflow;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;

import contrail.sequences.FastQRecord;
import contrail.sequences.Read;

/**
 * Transforms for processing reads.
 */
public class ReadTransforms {
  /**
   * Rekey the reads by the id.
   */
  public static class KeyFastQByIdDo
      extends DoFn<FastQRecord, KV<String, FastQRecord>> {
    // TODO(jeremy@lewi.us): This probably belongs in FastQTransforms.
    @Override
    public void processElement(ProcessContext c) {
      FastQRecord read = c.element();
      c.output(KV.of(read.getId().toString(), read));
    }
  }

  /**
   * Rekey the read by the id.
   *
   * Read is filtered out if no id is available.
   */
  public static class KeyByReadIdDo
      extends DoFn<Read, KV<String, Read>> {

    @Override
    public void processElement(ProcessContext c) {
      Read read = c.element();
      String readId = "";
      if (read.getFastq() != null) {
        readId = read.getFastq().getId().toString();
      }

      if (readId.isEmpty()) {
        // TODO(jeremy@lewi.us): Add a counter to keep track of reads with no
        // id once Dataflow supports that.
        // this.increment("contrail", "ReadDoFns-KeyByIdDo-read-no-id");
        return;
      }
      c.output(KV.of(readId, read));
    }
  }
}
