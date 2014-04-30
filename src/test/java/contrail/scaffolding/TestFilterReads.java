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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.crunch.PCollection;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.PipelineResult.StageResult;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.sequences.MatePair;
import contrail.sequences.ReadIdUtil;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

public class TestFilterReads {
  Random generator = new Random();

  // Create a mapping aligning the read to the contig.
  private BowtieMapping createMapping(String contigId, String readId) {
    BowtieMapping mapping = new BowtieMapping();
    mapping.setNumMismatches(0);
    mapping.setReadId(readId);
    mapping.setRead("");
    mapping.setReadClearEnd(0);
    mapping.setReadClearStart(0);
    mapping.setContigStart(0);
    mapping.setContigEnd(100);
    mapping.setContigId(contigId);

    return mapping;
  }

  private FastQRecord createRead(String readId) {
    FastQRecord read = new FastQRecord();

    read.setId(readId);
    read.setQvalue("");

    read.setRead(AlphabetUtil.randomString(
        generator, 10, DNAAlphabetFactory.create()));
    return read;
  }

  @Test
  public void testReadFiltering() {
    // Test to make sure reads are properly filtered.
    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();

    for (String readId : new String[] {"read1", "read4"}) {
      BowtieMapping mapping = createMapping("contig_1", readId);
      mappings.add(mapping);
    }

    ArrayList<FastQRecord> reads = new ArrayList<FastQRecord>();

    for (String readId : new String[] {"read1", "read2", "read3", "read4"}) {
      reads.add(createRead(readId));
    }

    PCollection<BowtieMapping> mappingsCollection =
        MemPipeline.typedCollectionOf(
            Avros.specifics(BowtieMapping.class), mappings);

    PCollection<FastQRecord> readsCollection =
        MemPipeline.typedCollectionOf(
            Avros.specifics(FastQRecord.class), reads);

    FilterReads stage = new FilterReads();

    PCollection<FastQRecord> filtered = stage.buildFilterPipeline(
        mappingsCollection, readsCollection);

    Iterable<FastQRecord> materializedOutput = filtered.materialize();
    HashMap<String, FastQRecord> actual = new HashMap<String, FastQRecord>();
    for (FastQRecord mapping : materializedOutput) {
      actual.put(mapping.getId().toString(), mapping);
    }
    assertEquals(2, actual.size());
    assertEquals(reads.get(0), actual.get("read1"));
    assertEquals(reads.get(3), actual.get("read4"));
  }

  @Test
  public void testReadFilteringUnMatched() {
    // Test that if reads aligned to a mapping aren't found that we increment
    // the appropriate counter.
    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();

    for (String readId : new String[] {"read1", "read4"}) {
      BowtieMapping mapping = createMapping("contig_1", readId);
      mappings.add(mapping);
    }

    ArrayList<FastQRecord> reads = new ArrayList<FastQRecord>();

    PCollection<BowtieMapping> mappingsCollection =
        MemPipeline.typedCollectionOf(
            Avros.specifics(BowtieMapping.class), mappings);

    PCollection<FastQRecord> readsCollection =
        MemPipeline.typedCollectionOf(
            Avros.specifics(FastQRecord.class), reads);

    FilterReads stage = new FilterReads();

    PCollection<FastQRecord> filtered = stage.buildFilterPipeline(
        mappingsCollection, readsCollection);

    long unMatched = 0L;
    PipelineResult result = MemPipeline.getInstance().done();
    final String groupName = "Contrail-Errors";
    final String counterName = "ExtractReads-UnMatched-Mappings";
    for (StageResult stageResult : result.getStageResults()) {
      if (!stageResult.getCounterNames().containsKey(groupName)) {
        continue;
      }
      if (!stageResult.getCounterNames().get(groupName).contains(
          counterName)) {
        continue;
      }
      unMatched = stageResult.getCounterValue(groupName, counterName);
    }

    assertEquals(2, unMatched);
  }

  private void writeFastQFile(String path, List<FastQRecord> records)
      throws IOException {
    FileOutputStream stream = new FileOutputStream(path);

    for (FastQRecord record : records) {
      FastUtil.writeFastQRecord(stream, record);
    }
    stream.close();
  }

  @Test
  public void testEndToEnd() throws IOException {
   // End to end test of the FilterReads pipeline.
    String[] readNames = new String[] {
        "lib_1_read0/1", "lib_1_read0/2",
        "lib_1_read1/1", "lib_1_read1/2",
        "lib_2_read0/1", "lib_2_read0/2",
        "lib_2_read1/1", "lib_2_read1/2",
    };

    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();
    mappings.add(createMapping("contig_0", readNames[0]));
    mappings.add(createMapping("contig_1", readNames[1]));
    mappings.add(createMapping("contig_0", readNames[2]));
    mappings.add(createMapping("contig_1", readNames[3]));
    mappings.add(createMapping("contig_0", readNames[4]));
    mappings.add(createMapping("contig_1", readNames[5]));

    ArrayList<FastQRecord> reads = new ArrayList<FastQRecord>();

    for (String readId : readNames) {
      reads.add(createRead(readId));
    }

    String tempDir = FileHelper.createLocalTempDir().getAbsolutePath();

    String mappingsPath = FilenameUtils.concat(tempDir, "mappings.avro");
    String readsPath = FilenameUtils.concat(tempDir, "reads.avro");
    String outputPath = FilenameUtils.concat(tempDir, "output");

    Configuration conf = new Configuration();
    AvroFileUtil.writeRecords(conf, new Path(mappingsPath), mappings);
    writeFastQFile(readsPath, reads);

    FilterReads stage = new FilterReads();
    stage.setConf(conf);
    stage.setParameter("bowtie_alignments", mappingsPath);
    stage.setParameter("reads_fastq", readsPath);
    stage.setParameter(
        "read_id_parser", ReadIdUtil.ReadParserUsingUnderscore.class.getName());
    stage.setParameter("outputpath", outputPath);

    assertTrue(stage.execute());
    System.out.println("outputpath: " + outputPath);

    // Read the outputs.
    ArrayList<MatePair> lib1 = AvroFileUtil.readRecords(
        FilenameUtils.concat(outputPath, "lib_1/part-r-00000.avro"),
        new MatePair().getSchema());
    assertEquals(2, lib1.size());

    // Check the reads aren't the same in the mate pair.
    for (MatePair pair : lib1) {
      String leftId = pair.getLeft().getId().toString();
      String rightId = pair.getRight().getId().toString();
      assertFalse(leftId.equals(rightId));

      String leftSequence = pair.getLeft().getRead().toString();
      String rightSequence = pair.getRight().getRead().toString();
      assertFalse(leftSequence.equals(rightSequence));
    }

    ArrayList<MatePair> lib2 = AvroFileUtil.readRecords(
        FilenameUtils.concat(outputPath, "lib_2/part-r-00000.avro"),
        new MatePair().getSchema());
    assertEquals(1, lib2.size());
  }
}
