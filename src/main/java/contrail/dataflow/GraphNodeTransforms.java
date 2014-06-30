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

import contrail.graph.GraphNodeData;

/**
 * Transforms for processing reads.
 */
public class GraphNodeTransforms {
  /**
   * Rekey the nodes by the node id.
   */
  public static class KeyByNodeId
      extends DoFn<GraphNodeData, KV<String, GraphNodeData>> {
    @Override
    public void processElement(ProcessContext c) {
      GraphNodeData node = c.element();
      c.output(KV.of(node.getNodeId().toString(), node));
    }
  }
}
