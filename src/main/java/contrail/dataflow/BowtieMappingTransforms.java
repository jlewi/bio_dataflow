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

import contrail.scaffolding.BowtieMapping;

/**
 * Transforms for processing BowtieMappings.
 */
public class BowtieMappingTransforms {
  public static class KeyByReadId
      extends DoFn<BowtieMapping, KV<String, BowtieMapping>> {

    @Override
    public void processElement(ProcessContext c) {
      BowtieMapping mapping = c.element();
      c.output(KV.of(mapping.getReadId().toString(), mapping));
    }
  }
}
