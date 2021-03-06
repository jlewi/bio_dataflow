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

import java.util.Arrays;
import java.util.List;

import org.apache.avro.specific.SpecificRecordBase;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;

import contrail.graph.GraphError;
import contrail.graph.GraphNodeData;
import contrail.graph.ValidateMessage;
import contrail.scaffolding.BowtieMapping;
import contrail.sequences.Read;

public class DataflowUtil {
  /**
   * Register default coders for our avro data structures.
   * @param p
   */
  public static void registerAvroCoders(Pipeline p) {
    List<Class<? extends SpecificRecordBase>> classes = Arrays.asList(
        BowtieMapping.class,
        GraphError.class,
        GraphNodeData.class,
        Read.class,
        ValidateMessage.class
        );

    CoderRegistry registry = p.getCoderRegistry();
    for (Class t : classes) {
      p.getCoderRegistry().registerCoder(
          t,
          registry.new ConstantCoderFactory(AvroCoder.of(t)));
    }
  }
}
