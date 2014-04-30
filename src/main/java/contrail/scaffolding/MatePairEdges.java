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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.specific.SpecificData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.crunch.ContigEdge;
import contrail.graph.GraphNodeData;
import contrail.graph.GraphUtil;
import contrail.scaffolding.BowtieDoFns.BuildMatePairMappings;
import contrail.scaffolding.BowtieDoFns.KeyByMateIdDo;
import contrail.stages.CrunchStage;
import contrail.stages.ParameterDefinition;

/**
 * Compute records describing the edges between contigs formed by mate pairs.
 */
public class MatePairEdges extends CrunchStage {
  /**
   * Creates the custom definitions that we need for this phase
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
            "graph_path",
            "The hdfs path to the avro files containing the GraphNodes " +
            "representing the contigs.",
            String.class, null);

    ParameterDefinition outputPath =
        new ParameterDefinition(
            "outputpath", "The directory to write the outputs which are " +
            "the files to pass to bambus for scaffolding.",
            String.class, null);

    for (ParameterDefinition def: Arrays.asList(
         bowtieAlignments, graphPath, outputPath)) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Form the ContigEdges.
   */
  public static class BuildContigEdges
      extends DoFn<MatePairMappings, ContigEdge> {
    @Override
    public void process(
        MatePairMappings mappings, Emitter<ContigEdge> emitter) {
      for (BowtieMapping left : mappings.getLeftMappings()) {
        for (BowtieMapping right : mappings.getRightMappings()) {
          ContigEdge edge = new ContigEdge();
          BowtieMapping major = null;
          BowtieMapping minor = null;
          CharSequence majorId = GraphUtil.computeMajorID(
              left.getContigId(), right.getContigId());

          if (left.getContigId().equals(majorId.toString())) {
            major = left;
            minor = right;
          } else {
            major = right;
            minor = left;
          }

          edge.setMajorContigId(major.getContigId().toString());
          edge.setMinorContigId(minor.getContigId().toString());
          edge.setNumLinks(1);
          emitter.emit(edge);
        }
      }
    }
  }

  /**
   * Key edges by major minor id so we can combine them.
   */
  public static class KeyByContigIds
      extends DoFn<ContigEdge, Pair<String, ContigEdge>> {
    @Override
    public void process(
        ContigEdge edge, Emitter<Pair<String, ContigEdge>> emitter) {
      String key =
          edge.getMajorContigId().toString() + "-"  + edge.getMinorContigId();
      emitter.emit(new Pair<String, ContigEdge>(key, edge));
    }
  }

  /**
   * Combine edges between the same contigs.
   */
  public static class CombineEdges
      extends DoFn<Pair<String, Iterable<ContigEdge>>, ContigEdge> {
    @Override
    public void process(
        Pair<String, Iterable<ContigEdge>> pair, Emitter<ContigEdge> emitter) {
      ContigEdge output = new ContigEdge();

      Iterator<ContigEdge> iterator = pair.second().iterator();
      ContigEdge edge = iterator.next();
      output.setMajorContigId(edge.getMajorContigId());
      output.setMinorContigId(edge.getMinorContigId());
      output.setNumLinks(edge.getNumLinks());

      while (iterator.hasNext()) {
        edge = iterator.next();
        output.setNumLinks(output.getNumLinks() + edge.getNumLinks());
      }
      emitter.emit(output);
    }
  }

  private static enum WhichEdge {
    MINOR,
    MAJOR,
  };

  /**
   * Key by edge id.
   */
  public static class KeyContigEdgeById
      extends DoFn<ContigEdge, Pair<String, ContigEdge>> {
    private WhichEdge terminal;

    public KeyContigEdgeById(WhichEdge terminal) {
      this.terminal = terminal;
    }

    @Override
    public void process(ContigEdge input,
        Emitter<Pair<String, ContigEdge>> emitter) {
      if (terminal == WhichEdge.MAJOR) {
        emitter.emit(new Pair<String, ContigEdge>(
            input.getMajorContigId().toString(), input));
      } else {
        emitter.emit(new Pair<String, ContigEdge>(
            input.getMinorContigId().toString(), input));
      }
    }
  }
  /**
   * For graph node emit a pair consisting of the contig id and the contig
   * length.
   */
  public static class ContigLengthsDo
      extends DoFn<GraphNodeData, Pair<String, Integer>> {
    @Override
    public void process(
        GraphNodeData node, Emitter<Pair<String, Integer>> emitter) {
      emitter.emit(
          new Pair<String, Integer>(node.getNodeId().toString(),
                                    node.getSequence().getLength()));
    }
  }

  /**
   * Attach the length of one of the contigs to the edge.
   */
  private static class JoinContigLengthDo extends
    DoFn<Pair<String, Pair<Integer, ContigEdge>>, ContigEdge> {
    private WhichEdge terminal;

    public JoinContigLengthDo(WhichEdge terminal) {
      this.terminal = terminal;
    }

    @Override
    public void process(Pair<String, Pair<Integer, ContigEdge>> input,
        Emitter<ContigEdge> emitter) {
      Integer length = input.second().first();
      if (length == null) {
        length = -1;
        this.increment("Contrail-Errors", "JoinContigLengthDo-No-Length");
      }
      ContigEdge edge = input.second().second();
      edge = SpecificData.get().deepCopy(edge.getSchema(), edge);
      if (terminal == WhichEdge.MAJOR) {
        edge.setMajorContigLength(length);
      } else {
        edge.setMinorContigLength(length);
      }
      emitter.emit(edge);
    }
  }

  /**
   * Join the edge stats with the length of a contig.
   * @param edgeStats
   * @param contigLengths
   * @param terminal
   * @return
   */
  private PCollection<ContigEdge> joinWithEdgeLength(
      PCollection<ContigEdge> edgeStats,
      PTable<String, Integer> contigLengths,
      WhichEdge terminal) {
    PTable<String, ContigEdge> edgeTable =
        edgeStats.parallelDo(new KeyContigEdgeById(terminal), Avros.tableOf(
            Avros.strings(), Avros.specifics(ContigEdge.class)));

    // We use the contig length as left dataset because each contig should
    // have a single length but a given contig could participate in multiple
    // ContigEdge's.
    JoinStrategy<String, Integer, ContigEdge> strategy =
        new DefaultJoinStrategy<String, Integer, ContigEdge>();

    // We do a right join because if there is a contig edge with no matching
    // contig thats an error and we want to track that.
    PTable<String, Pair<Integer, ContigEdge>> joined =
        strategy.join(contigLengths, edgeTable, JoinType.RIGHT_OUTER_JOIN);

    return joined.parallelDo(
        new JoinContigLengthDo(terminal), Avros.specifics(ContigEdge.class));
  }

  @Override
  protected void stageMain() {
    String bowtiePath = (String) stage_options.get("bowtie_alignments");
    String graphPath = (String) stage_options.get("graph_path");
    String outputPath = (String) stage_options.get("outputpath");

    Source<BowtieMapping> mappingSource = From.avroFile(
        bowtiePath, Avros.specifics(BowtieMapping.class));

    deleteExistingPath(new Path(outputPath));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(MatePairEdges.class, getConf());

    PCollection<BowtieMapping> mappings = pipeline.read(mappingSource);

    PGroupedTable<String, BowtieMapping> alignments = mappings.parallelDo(
        new KeyByMateIdDo(), Avros.tableOf(
            Avros.strings(),
                Avros.specifics(BowtieMapping.class))).groupByKey();

    PCollection<MatePairMappings>  pairedMappings = alignments.parallelDo(
        new BuildMatePairMappings(), Avros.specifics(MatePairMappings.class));

    PCollection<ContigEdge> edges = pairedMappings.parallelDo(
        new BuildContigEdges(), Avros.specifics(ContigEdge.class));

    PGroupedTable<String, ContigEdge> groupedEdges = edges.parallelDo(
        new KeyByContigIds(), Avros.tableOf(
            Avros.strings(), Avros.specifics(ContigEdge.class))).groupByKey();

    PCollection<ContigEdge> edgeStats = groupedEdges.parallelDo(
        new CombineEdges(), Avros.specifics(ContigEdge.class));

    Source<GraphNodeData> graphSource = From.avroFile(
        graphPath, Avros.specifics(GraphNodeData.class));

    PCollection<GraphNodeData> graphNodes = pipeline.read(graphSource);

    // Create a mapping from contig_id -> length.
    PTable<String, Integer> nodeLength = graphNodes.parallelDo(
        new ContigLengthsDo(), Avros.tableOf(Avros.strings(), Avros.ints()));


    edgeStats = joinWithEdgeLength(edgeStats, nodeLength, WhichEdge.MAJOR);
    edgeStats = joinWithEdgeLength(edgeStats, nodeLength, WhichEdge.MINOR);

    // TODO(jlewi): Key the contig edges by minor edge and join to get length
    // of minor edge. Then repeat with major edge
    edgeStats.write(To.avroFile(outputPath));

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new MatePairEdges(), args);
    System.exit(res);
  }
}
