/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)

package contrail.scaffolding;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.ContigReadAlignment;
import contrail.graph.GraphNodeData;
import contrail.sequences.Read;
import contrail.stages.ContrailParameters;
import contrail.stages.CrunchStage;
import contrail.stages.ParameterDefinition;

/*
 * Form (Contig, Read) pairs based on the bowtie alignments.
 *
 * The intention is to validate the alignments produced by bowtie.
 * Using the alignment we should compute the number of errors and overlap
 * between the read and the contig.
 */
public class JoinContigsAndReads extends CrunchStage {
  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition bowtieAlignments =
        new ParameterDefinition(
            "bowtie_alignments",
            "The hdfs path to the avro files containing the alignments " +
            "produced by bowtie of the reads to the contigs.",
            String.class, null);

    ParameterDefinition graphPath =
        new ParameterDefinition(
            "contigs", "The glob on the hadoop filesystem to the avro " +
            "files containing the GraphNodeData records representing the " +
            "graph.", String.class, null);

    ParameterDefinition readsPath =
        new ParameterDefinition(
            "reads", "The glob on the hadoop filesystem to the avro " +
            "files containing the reads.", String.class, null);

    for (ParameterDefinition def : ContrailParameters.getInputOutputPathOptions() ){
      if (def.getName().equals("outputpath")) {
        defs.put(def.getName(), def);
      }
    }

    defs.put(bowtieAlignments.getName(), bowtieAlignments);
    defs.put(graphPath.getName(), graphPath);
    defs.put(readsPath.getName(), readsPath);

    return Collections.unmodifiableMap(defs);
  }

  /**
   * Join each bowtie mapping with the read it is aligned to.
   * @param mappings
   * @param reads
   * @return
   */
  protected PCollection<ContigReadAlignment> joinMappingsAndReads(PCollection<BowtieMapping> mappings,
      PCollection<Read> reads) {
    PTable<String, BowtieMapping> keyedMappings = mappings.parallelDo(
        new BowtieDoFns.KeyByReadIdDo(),
        Avros.tableOf(Avros.strings(), Avros.specifics(BowtieMapping.class)));

    PTable<String, Read> keyedReads = reads.parallelDo(
        new ReadDoFns.KeyByIdDo(),
        Avros.tableOf(Avros.strings(),  Avros.specifics(Read.class)));


  }

  @Override
  protected void stageMain() {
    String outputPath = (String) stage_options.get("outputpath");
    deleteExistingPath(new Path(outputPath));

    String bowtiePath = (String) stage_options.get("bowtie_alignments");
    Source<BowtieMapping> bowtieMappingSource = From.avroFile(
        bowtiePath, Avros.specifics(BowtieMapping.class));

    String readPath = (String) stage_options.get("reads");
    Source<Read> readSource = From.avroFile(readPath, Avros.specifics(Read.class));

    String contigsPath = (String) stage_options.get("contigs");
    Source<GraphNodeData> nodeSource = From.avroFile(
        contigsPath, Avros.specifics(GraphNodeData.class));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(JoinContigsAndReads.class, getConf());

    PCollection<GraphNodeData> contigs = pipeline.read(nodeSource);
    PCollection<Read> reads = pipeline.read(readSource);
    PCollection<BowtieMapping> alignments = pipeline.read(bowtieMappingSource);

    // Group the bowtie alignments with the read.
    PCollection<ContigReadAlignment> readMappingsPair = joinMappingsAndReads(
        alignments,reads);

    // Group the (BowtieMapping, Read) pairs with the Contigs.
    PTable<String, GraphNodeData> contigsTable;
    PTable<String, ContigReadAlignments> contigMapToAlignments;

    // Do the join.
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new FilterBowtieAlignments(), args);
    System.exit(res);
  }
}
