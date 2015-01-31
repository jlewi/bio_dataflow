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
import contrail.scaffolding.BowtieParser;
import contrail.sequences.ReadIdUtil;

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

  public static class KeyByMatedId
      extends DoFn<BowtieMapping, KV<String, BowtieMapping>> {
    @Override
    public void processElement(ProcessContext c) {
      BowtieMapping mapping = c.element();
      String mateId = ReadIdUtil.getMateId(mapping.getReadId().toString());
      c.output(KV.of(mateId, mapping));
    }
  }

  /**
   * Convert a line of text produced by bowtie into a BowtieMapping record.
   */
  public static class ParseMappingLineDo extends DoFn<String, BowtieMapping> {
    private transient BowtieParser parser;

    @Override
    public void startBundle(Context c) {
      parser = new BowtieParser();
    }

    @Override
    public void processElement(ProcessContext c) {
      BowtieMapping mapping = new BowtieMapping();
      parser.parse(c.element(), mapping);
      c.output(mapping);
    }
  }
}
