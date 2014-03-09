/**
 * Licensed under the Apache License, Version 2.0 (the "License");
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.avro.specific.SpecificData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.graph.GraphUtil;
import contrail.scaffolding.BowtieDoFns.BuildMatePairMappings;
import contrail.scaffolding.BowtieDoFns.KeyByMateIdDo;
import contrail.stages.ContrailParameters;
import contrail.stages.CrunchStage;
import contrail.stages.ParameterDefinition;

/**
 * Remove all bowtie mappings if:
 * 1) Both reads align to the same mate pair.
 * 2) One of the reads in the pair doesn't align to a contig.
 *
 * We also group all the alignments by the contigs they link. If a link
 * is supported by a single mate pair we remove the bowtie alignments to remove
 * the link.
 *
 * We then group the mappings based on the contigs they link. We remove
 * all links that are supported by a single mate pair.
 */
public class FilterBowtieAlignments extends CrunchStage {
  /**
   *  creates the custom definitions that we need for this phase
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
         ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Filter the mate pairs.
   */
  public static class FilterMatePairMappings
    extends DoFn<MatePairMappings, MatePairMappings> {
    @Override
    public void process(
        MatePairMappings input,
        Emitter<MatePairMappings> emitter) {

      if (input.getLeftMappings().size() == 0 ||
          input.getRightMappings().size() == 0) {
        this.increment("Contrail-FilterMatePairMappings", "single-reads");
        // Filter it out because both reads align to the same contig.
        return;
      }

      // Check if its a cycle.
      HashSet<String> contigIds = new HashSet<String>();
      for (int i = 0; i < 2; ++i) {
        List<BowtieMapping> mappings = input.getLeftMappings();
        if (i == 1) {
          mappings = input.getRightMappings();
        }
        for (BowtieMapping m : mappings) {
          contigIds.add(m.getContigId().toString());
        }
      }

      if (contigIds.size() == 1) {
        this.increment("Contrail-FilterMatePairMappings", "cycle");
        return;
      }

      emitter.emit(input);
    }
  }

  /**
   * Key edges by major minor id so we can combine them.
   */
  public static class KeyByContigLinkId
      extends DoFn<MatePairMappings, Pair<String, MatePairMappings>> {
    @Override
    public void process(
        MatePairMappings mappings,
        Emitter<Pair<String, MatePairMappings>> emitter) {
      for (BowtieMapping left : mappings.getLeftMappings()) {
        for (BowtieMapping right : mappings.getRightMappings()) {
          CharSequence majorId = GraphUtil.computeMajorID(
              left.getContigId(), right.getContigId());
          CharSequence minorId = GraphUtil.computeMinorID(
              left.getContigId(), right.getContigId());
          String key = majorId.toString() + "-"  + minorId.toString();

          MatePairMappings output = new MatePairMappings();
          output.setLibraryId(mappings.getLibraryId());
          output.setMateId(mappings.getMateId());
          output.setLeftMappings(new ArrayList<BowtieMapping>());
          output.setRightMappings(new ArrayList<BowtieMapping>());
          output.getLeftMappings().add(left);
          output.getRightMappings().add(right);
          emitter.emit(new Pair<String, MatePairMappings>(key, output));
        }
      }
    }
  }

  /**
   * Remove all links supported by a link mate pair.
   */
  public static class FilterContigLinks
      extends DoFn<Pair<String, Iterable<MatePairMappings>>, MatePairMappings> {
    @Override
    public void process(Pair<String, Iterable<MatePairMappings>> links,
        Emitter<MatePairMappings> emitter) {
      ArrayList<MatePairMappings> mappings = new ArrayList<MatePairMappings>();
      for (MatePairMappings m : links.second()) {
        mappings.add(SpecificData.get().deepCopy(m.getSchema(), m));
      }
      if (mappings.size() <= 1) {
        // Filter out these mappings.
        return;
      }

      this.increment("contrail-FilterContigLinks", "good-links");
      for (MatePairMappings m : mappings) {
        this.increment("contrail-FilterContigLinks", "good-mate-pairs");
        emitter.emit(m);
      }
    }
  }

  /**
   * Extract the bowtie mappings.
   */
  public static class ExtractMappings
      extends DoFn<MatePairMappings, BowtieMapping> {
    @Override
    public void process(
        MatePairMappings input,
        Emitter<BowtieMapping> emitter) {

      for (int i = 0; i < 2; ++i) {
        List<BowtieMapping> mappings = input.getLeftMappings();
        if (i == 1) {
          mappings = input.getRightMappings();
        }
        for (BowtieMapping m : mappings) {
          emitter.emit(m);
        }
      }
    }
  }

  public static PCollection<MatePairMappings> buildMatePairs(
      PCollection<BowtieMapping> inputMappings) {
    PGroupedTable<String, BowtieMapping> alignments = inputMappings.parallelDo(
        new KeyByMateIdDo(), Avros.tableOf(
            Avros.strings(),
                Avros.specifics(BowtieMapping.class))).groupByKey();

    PCollection<MatePairMappings>  pairedMappings = alignments.parallelDo(
        new BuildMatePairMappings(), Avros.specifics(MatePairMappings.class));

    return pairedMappings;
  }


  public static PCollection<MatePairMappings> filterMatePairs(
      PCollection<MatePairMappings> pairedMappings) {
    PCollection<MatePairMappings> filtered = pairedMappings.parallelDo(
        new FilterMatePairMappings(), Avros.specifics(MatePairMappings.class));

    return filtered;
  }

  public static PCollection<MatePairMappings> filterLinks(
      PCollection<MatePairMappings> mappings) {
    PGroupedTable<String, MatePairMappings> groupedLinks = mappings.parallelDo(
        new KeyByContigLinkId(), Avros.tableOf(
            Avros.strings(), Avros.specifics(
                MatePairMappings.class))).groupByKey();

    PCollection<MatePairMappings> filteredMappings = groupedLinks.parallelDo(
        new FilterContigLinks(), Avros.specifics(MatePairMappings.class));

    return filteredMappings;
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    Source<BowtieMapping> source = From.avroFile(
        inputPath, Avros.specifics(BowtieMapping.class));

    deleteExistingPath(new Path(outputPath));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(FilterBowtieAlignments.class, getConf());

    PCollection<BowtieMapping> raw = pipeline.read(source);

    PCollection<MatePairMappings> mappings = buildMatePairs(raw);
    PCollection<MatePairMappings> filtered =
        filterLinks(filterMatePairs(mappings));

    PCollection<BowtieMapping> outputs = filtered.parallelDo(
        new ExtractMappings(), Avros.specifics(BowtieMapping.class));

    outputs.write(To.avroFile(outputPath));

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new FilterBowtieAlignments(), args);
    System.exit(res);
  }
}
