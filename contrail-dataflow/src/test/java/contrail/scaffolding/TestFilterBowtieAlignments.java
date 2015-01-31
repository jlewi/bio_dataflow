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

import java.util.ArrayList;
import java.util.Collections;

import org.apache.avro.specific.SpecificData;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import contrail.scaffolding.FilterBowtieAlignments.ExtractMappings;

public class TestFilterBowtieAlignments {
  @Test
  public void testFilterMatePairs() {
    // Test to make sure mappings are properly filtered.

    ArrayList<BowtieMapping> mappings = new ArrayList<BowtieMapping>();

    ArrayList<BowtieMapping> expectedOutput = new ArrayList<BowtieMapping>();
    // Mate pair to keep.
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId/1");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);

      expectedOutput.add(SpecificData.get().deepCopy(
          mapping.getSchema(), mapping));
    }

    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId/2");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig2");
      mappings.add(mapping);

      expectedOutput.add(SpecificData.get().deepCopy(
          mapping.getSchema(), mapping));
    }

    // Mate pair with only one aligned read.
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId2/1");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);
    }


    // Both reads align to same contig.
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId3/1");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);
    }

    {
      BowtieMapping mapping = new BowtieMapping();
      mapping.setNumMismatches(0);
      mapping.setReadId("library.readId3/2");
      mapping.setRead("");
      mapping.setReadClearEnd(0);
      mapping.setReadClearStart(0);
      mapping.setContigStart(0);
      mapping.setContigEnd(100);
      mapping.setContigId("contig1");
      mappings.add(mapping);
    }

    PCollection<BowtieMapping> input =
        MemPipeline.typedCollectionOf(
            Avros.specifics(BowtieMapping.class),
                mappings);

    PCollection<BowtieMapping> outputs =
        FilterBowtieAlignments.filterMatePairs(
            FilterBowtieAlignments.buildMatePairs(input)).parallelDo(
            new ExtractMappings(), Avros.specifics(BowtieMapping.class));

    Iterable<BowtieMapping> materializedOutput = outputs.materialize();
    ArrayList<BowtieMapping> actual = new ArrayList<BowtieMapping>();
    for (BowtieMapping mapping : materializedOutput) {
      actual.add(mapping);
    }
    Collections.reverse(expectedOutput);
    assertEquals(expectedOutput, actual);
  }

  @Test
  public void testFilterLinks() {
    // Run the entire pipeline to make sure links with insufficient support
    // are properly filtered.

    ArrayList<MatePairMappings> input = new ArrayList<MatePairMappings>();
    ArrayList<MatePairMappings> expectedOutput = new ArrayList<MatePairMappings>();
    // Mate pair to keep.
    {
      for (int i = 0; i < 2; ++i) {
        MatePairMappings mappings = new MatePairMappings();
        mappings.setLibraryId("");
        mappings.setMateId("library.readId");
        mappings.setLeftMappings(new ArrayList<BowtieMapping>());
        mappings.setRightMappings(new ArrayList<BowtieMapping>());

        BowtieMapping left = new BowtieMapping();
        left.setNumMismatches(0);
        left.setReadId(String.format("library.readId-%d/1", i));
        left.setRead("");
        left.setReadClearEnd(0);
        left.setReadClearStart(0);
        left.setContigStart(0);
        left.setContigEnd(100);
        left.setContigId("contig1");

        BowtieMapping right = new BowtieMapping();
        right.setNumMismatches(0);
        right.setReadId(String.format("library.readId-%d/2", i));
        right.setRead("");
        right.setReadClearEnd(0);
        right.setReadClearStart(0);
        right.setContigStart(0);
        right.setContigEnd(100);
        right.setContigId("contig1");

        mappings.getLeftMappings().add(left);
        mappings.getRightMappings().add(right);

        input.add(mappings);

        expectedOutput.add(SpecificData.get().deepCopy(
            mappings.getSchema(), mappings));
      }
    }

    // Link to filter.
    {
      MatePairMappings mappings = new MatePairMappings();
      mappings.setLibraryId("");
      mappings.setMateId("library.readId");
      mappings.setLeftMappings(new ArrayList<BowtieMapping>());
      mappings.setRightMappings(new ArrayList<BowtieMapping>());

      BowtieMapping left = new BowtieMapping();
      left.setNumMismatches(0);
      left.setReadId("library.readId-bad/1");
      left.setRead("");
      left.setReadClearEnd(0);
      left.setReadClearStart(0);
      left.setContigStart(0);
      left.setContigEnd(100);
      left.setContigId("contig2");

      BowtieMapping right = new BowtieMapping();
      right.setNumMismatches(0);
      right.setReadId("library.readId-bad/2");
      right.setRead("");
      right.setReadClearEnd(0);
      right.setReadClearStart(0);
      right.setContigStart(0);
      right.setContigEnd(100);
      right.setContigId("contig2");

      mappings.getLeftMappings().add(left);
      mappings.getRightMappings().add(right);

      input.add(mappings);
    }

    PCollection<MatePairMappings> inputCollection =
        MemPipeline.typedCollectionOf(
            Avros.specifics(MatePairMappings.class),
                input);

    PCollection<MatePairMappings> outputs =
        FilterBowtieAlignments.filterLinks(inputCollection);

    Iterable<MatePairMappings> materializedOutput = outputs.materialize();
    ArrayList<MatePairMappings> actual = new ArrayList<MatePairMappings>();
    for (MatePairMappings mapping : materializedOutput) {
      actual.add(mapping);
    }
    assertEquals(expectedOutput, actual);
  }
}
