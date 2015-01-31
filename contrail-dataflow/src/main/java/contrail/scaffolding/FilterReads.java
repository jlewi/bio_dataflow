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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.impl.mr.run.RuntimeParameters;
import org.apache.crunch.io.From;
import org.apache.crunch.io.avro.AvroPathPerKeyTarget;
import org.apache.crunch.lib.join.DefaultJoinStrategy;
import org.apache.crunch.lib.join.JoinStrategy;
import org.apache.crunch.lib.join.JoinType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.ToolRunner;

import contrail.io.FastQWritable;
import contrail.io.mapreduce.FastQInputFormatNew;
import contrail.sequences.FastQDoFns;
import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.sequences.MatePairDoFns;
import contrail.stages.ContrailParameters;
import contrail.stages.CrunchStage;
import contrail.stages.ParameterDefinition;
import contrail.util.ContrailLogger;

/**
 * Filter the reads.
 *
 * We filter the original reads because the Bambus scaffolder needs to hold
 * in memory a map containing the ids of all reads.
 */
public class FilterReads extends CrunchStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(FilterReads.class);
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
      if (def.getName().equals("outputpath")) {
        defs.put(def.getName(), def);
      }
    }

    ParameterDefinition bowtieAlignments =
        new ParameterDefinition(
            "bowtie_alignments",
            "The hdfs path to the avro files containing the alignments " +
            "produced by bowtie of the reads to the contigs.",
            String.class, null);

    ParameterDefinition reads =
        new ParameterDefinition(
            "reads_fastq",
            "The hdfs path to fastq files containing the reads.",
            String.class, null);

    ParameterDefinition parserClass =
        new ParameterDefinition(
            "read_id_parser",
            "The fully qualified name of the class which parses read ids.",
            String.class, null);

    defs.put(bowtieAlignments.getName(), bowtieAlignments);
    defs.put(reads.getName(), reads);
    defs.put(parserClass.getName(), parserClass);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Turn the read ids into a table.
   */
  public static class BuildStringTable
      extends DoFn<String, Pair<String, String>> {
    @Override
    public void process(String input,
        Emitter<Pair<String, String>> emitter) {
      // For the value just use an empty string to avoid wasting space.
      emitter.emit(new Pair(input, ""));
    }
  }

  /**
   * Extract the reads.
   */
  public static class ExtractReads
      extends DoFn<Pair<String, Pair<String, FastQRecord>>, FastQRecord> {
    @Override
    public void process(Pair<String, Pair<String, FastQRecord>> input,
        Emitter<FastQRecord> emitter) {
      if (input.second().second() == null) {
        increment("Contrail-Errors", "ExtractReads-UnMatched-Mappings");
        return;
      }

      emitter.emit(input.second().second());
    }
  }

  /**
   * Build the filtering pipeline.
   */
  protected PCollection<FastQRecord> buildFilterPipeline(
      PCollection<BowtieMapping> mappings,
      PCollection<FastQRecord> reads) {
    PCollection<String> mappingIds = mappings.parallelDo(
        new BowtieDoFns.ReadIdDo(), Avros.strings());

    PTable<String, String> mappingIdsTable = mappingIds.parallelDo(
        new BuildStringTable(),  Avros.tableOf(
            Avros.strings(),
            Avros.strings()));

    PTable<String, FastQRecord> readsTable =
        reads.parallelDo(new ReadDoFns.KeyFastQByIdDo(), Avros.tableOf(
            Avros.strings(),
            Avros.specifics(FastQRecord.class)));

    JoinStrategy<String, String, FastQRecord> strategy =
        new DefaultJoinStrategy<String, String, FastQRecord>();

    // We do a left join because we want to count mappings that aren't
    // aligned to reads.
    PTable<String, Pair<String, FastQRecord>> joined =
        strategy.join(mappingIdsTable, readsTable, JoinType.LEFT_OUTER_JOIN);

    PCollection<FastQRecord> filteredReads = joined.parallelDo(
        new ExtractReads(), Avros.specifics(FastQRecord.class));
    return filteredReads;
  }

  /**
   * Join the reads by library id.
   */
  protected PTable<String, MatePair> formAndGroupMatePairs(
      PCollection<FastQRecord> reads, String parserClassName) {
    PCollection<MatePair> matePairs = FastQDoFns.buildMatePairs(reads);

    Class parserClass = null;
    try {
      parserClass = Class.forName(parserClassName);
    } catch (ClassNotFoundException e) {
      sLogger.fatal("Could not find ReadId Parser: " + parserClassName);
    }

    PTable<String, MatePair> library = matePairs.parallelDo(
        new MatePairDoFns.KeyMatePairByLibraryId(parserClass),
        Avros.tableOf(Avros.strings(), Avros.specifics(MatePair.class)));

    return library;
  }

  protected PCollection<FastQRecord> readFastQFiles(Pipeline pipeline) {
    String readsPath = (String) stage_options.get("reads_fastq");

    PCollection<FastQRecord> allReads = null;

    // TODO(jlewi): I don't think this is necessary. We should try passing
    // a comma separated list of globs.
    for (String path : readsPath.split(",")) {
      sLogger.info("Reading:" + path);
      TableSource<LongWritable, FastQWritable> source = From.formattedFile(
          path, FastQInputFormatNew.class, LongWritable.class,
          FastQWritable.class);

      // See https://issues.apache.org/jira/browse/CRUNCH-369
      // This issue doesn't appear to be fixed in 0.9.0 but should hopefully
      // be fixed in version 0.10.0.
      source.inputConf(
          RuntimeParameters.DISABLE_COMBINE_FILE, Boolean.TRUE.toString());

      PTable<LongWritable, FastQWritable> reads = pipeline.read(source);

      PCollection<FastQRecord> output = reads.values().parallelDo(
          new FastQDoFns.WritableToAvroDo(),
          Avros.specifics(FastQRecord.class));
      if (allReads == null) {
        allReads = output;
      } else {
        allReads = allReads.union(output);
      }
    }

    return allReads;
  }

  @Override
  protected void stageMain() {
    String bowtiePath = (String) stage_options.get("bowtie_alignments");
    String outputPath = (String) stage_options.get("outputpath");
    String readIdParser = (String) stage_options.get("read_id_parser");

    // TODO(jlewi): We should move this into a validation routine for the
    // parameter.
    Class parserClass = null;
    try {
      parserClass = Class.forName(readIdParser);
    } catch (ClassNotFoundException e) {
      sLogger.fatal("Could not find ReadId Parser: " + readIdParser);
    }

    deleteExistingPath(new Path(outputPath));

    Source<BowtieMapping> mappingSource = From.avroFile(
        bowtiePath, Avros.specifics(BowtieMapping.class));

    // Create an object to coordinate pipeline creation and execution.
    Pipeline pipeline = new MRPipeline(FilterReads.class, getConf());

    PCollection<FastQRecord> reads = readFastQFiles(pipeline);

    PCollection<BowtieMapping> mappings = pipeline.read(mappingSource);

    PCollection<FastQRecord> filteredReads =
        buildFilterPipeline(mappings, reads);

    PGroupedTable<String, MatePair> libraries = formAndGroupMatePairs(
        filteredReads, readIdParser).groupByKey();

    AvroPathPerKeyTarget target =
        new AvroPathPerKeyTarget(outputPath);
    libraries.write(target);

    // Execute the pipeline as a MapReduce.
    PipelineResult result = pipeline.done();

    printCounters(result);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new FilterReads(), args);
    System.exit(res);
  }
}
