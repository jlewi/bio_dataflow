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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.scaffolding.BowtieMapping;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;
import contrail.sequences.Sequence;
import contrail.stages.ContrailParameters;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.AvroFileUtil;

/**
 * Create some fake data for testing the JoinMappingsAndReads pipeline.
 */
public class CreateFakeData extends NonMRStage {
  Random generator = new Random();

  private BowtieMapping emptyMapping() {
    BowtieMapping mapping = new BowtieMapping();
    mapping.setContigId("");
    mapping.setContigStart(0);
    mapping.setContigEnd(0);
    mapping.setNumMismatches(0);
    mapping.setRead("");
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

  private GraphNode createNode(String nodeId, String sequence) {
    GraphNode node = new GraphNode();
    node.setNodeId(nodeId);
    node.setSequence(new Sequence(sequence, DNAAlphabetFactory.create()));
    return node;
  }

  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def : ContrailParameters.getInputOutputPathOptions() ){
      if (def.getName().equals("outputpath")) {
        defs.put(def.getName(), def);
      }
    }

    ParameterDefinition pipelineRunner =
        new ParameterDefinition(
            "pipeline_runner", "The pipeline runner to use.",
            String.class, "DirectPipelineRunner");

    return defs;
  }
  @Override
  protected void stageMain() {
    ArrayList<GraphNodeData> nodes = new ArrayList<GraphNodeData>();
    ArrayList<BowtieMapping> alignments = new ArrayList<BowtieMapping>();
    ArrayList<Read> reads = new ArrayList<Read>();

    for (int i = 0; i < 2; ++i) {
      String contigId = String.format("contig%02d",  i);
      GraphNode node = createNode(contigId, "ACTCG");
      nodes.add(node.getData());

      String readId = String.format("read%02d",  i);
      reads.add(randomRead(readId));

      BowtieMapping mapping = emptyMapping();
      mapping.setContigId(contigId);;
      mapping.setReadId(readId);
      alignments.add(mapping);
    }

    Path outputPath = new Path((String) stage_options.get("outputpath"));
    FileSystem fs;
    try {
      fs = outputPath.getFileSystem(getConf());
      if (fs.exists(outputPath)) {
        fs.delete(outputPath, true);
      }
      fs.mkdirs(outputPath);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    Path nodesPath = new Path(FilenameUtils.concat(
        outputPath.toString(), "nodes.avro"));
    Path alignmentsPath = new Path(FilenameUtils.concat(
        outputPath.toString(), "alignments.avro"));
    Path readsPath = new Path(FilenameUtils.concat(
        outputPath.toString(), "reads.avro"));

    AvroFileUtil.writeRecords(getConf(), nodesPath, nodes);
    AvroFileUtil.writeRecords(getConf(), alignmentsPath, alignments);
    AvroFileUtil.writeRecords(getConf(), readsPath, reads);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new CreateFakeData(), args);
    System.exit(res);
  }
}
