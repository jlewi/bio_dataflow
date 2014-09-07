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
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.avro.specific.SpecificData;
import org.junit.Test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.values.PCollection;

import contrail.dataflow.DataflowUtil;
import contrail.dataflow.JoinMappingsAndReads;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphTestUtil;
import contrail.scaffolding.BowtieMapping;
import contrail.scaffolding.ContigReadAlignment;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;

public class TestJoinContigsReadsMappings {
  Random generator = new Random();

  private BowtieMapping emptyMapping(String contigId, String readId) {
    BowtieMapping mapping = new BowtieMapping();
    mapping.setContigId(contigId);
    mapping.setContigStart(0);
    mapping.setContigEnd(0);
    mapping.setNumMismatches(0);
    mapping.setRead(readId);
    mapping.setReadClearEnd(0);
    mapping.setReadClearStart(0);
    mapping.setReadId("");

    return mapping;
  }

  private Read randomRead(String readId) {
    Read read = new Read();
    read.setFastq(new FastQRecord());
    read.getFastq().setId(readId);
    read.getFastq().setQvalue("");
    read.getFastq().setRead(AlphabetUtil.randomString(
        generator, 5, DNAAlphabetFactory.create()));

    return read;
  }

  @Test
  public void testJoinContigsReadsMappings() {
    ArrayList<GraphNodeData> nodes = new ArrayList<GraphNodeData>();
    ArrayList<Read> reads = new ArrayList<Read>();
    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();

    HashMap<String, ContigReadAlignment> expected =
        new HashMap<String, ContigReadAlignment>();

    // Create some triplets(node, read, mapping) and the expected result
    // of the join.
    for (int i = 0; i < 2; ++i) {
      String contigId = String.format("contig%02d",  i);
      GraphNode node = GraphTestUtil.createNode(contigId, "ACTCG");
      nodes.add(node.getData());

      String readId = String.format("read%02d",  i);
      Read read = randomRead(readId);
      reads.add(read);

      BowtieMapping mapping = emptyMapping(contigId, readId);
      mappings.add(mapping);

      ContigReadAlignment joined = new ContigReadAlignment();
      joined.setGraphNode(node.clone().getData());
      joined.setBowtieMapping(SpecificData.get().deepCopy(
          mapping.getSchema(), mapping));
      joined.setRead(SpecificData.get().deepCopy(read.getSchema(), read));

      expected.put(contigId + " " + readId, joined);
    }

    // TODO(jlewi): Should we add records for contigs, reads that have
    // no mappings?

    PipelineOptions options = new PipelineOptions();
    Pipeline p = Pipeline.create();
    DataflowUtil.registerAvroCoders(p);

    PCollection<GraphNodeData> nodesCollection =
        p.begin().apply(Create.of(nodes));

    PCollection<Read> readsCollection =
        p.begin().apply(Create.of(reads));

    PCollection<BowtieMapping> mappingsCollection =
        p.begin().apply(Create.of(mappings));

    JoinContigsReadsMappings join = new JoinContigsReadsMappings();

    JoinMappingsAndReads stage = new JoinMappingsAndReads();

    PCollection<ContigReadAlignment> joined = stage.joinNodes(
        alignmentsCollection, nodesCollection);

    DirectPipelineRunner runner = DirectPipelineRunner.fromOptions(options);
    DirectPipelineRunner.EvaluationResults result = p.run(runner);
    List<ContigReadAlignment> finalResults = result.getPCollection(joined);

  }
}
