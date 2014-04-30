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
import java.util.HashMap;
import java.util.Iterator;

import org.apache.avro.specific.SpecificData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import contrail.sequences.ReadIdUtil;

/**
 * A collection of DoFns for working with bowtie alignments.
 */
public class BowtieDoFns {
  /**
   * Key the bowtie mapping by the contig id.
   */
  public static class KeyByContigId
      extends DoFn<BowtieMapping, Pair<String, BowtieMapping>> {
    @Override
    public void process(
        BowtieMapping mapping,
        Emitter<Pair<String, BowtieMapping>> emitter) {
      emitter.emit(new Pair<String, BowtieMapping>(
          mapping.getContigId().toString(), mapping));
    }
  }

  /**
   * Build a record containing all the alignments for a given mate pair.
   *
   * Input is keyed by the id for a mate pair. The values are the alignments
   * of the reads associated with that mate pair to contigs.
   *
   */
  public static class BuildMatePairMappings
      extends DoFn<Pair<String, Iterable<BowtieMapping>>,
                   MatePairMappings> {
    @Override
    public void process(
        Pair<String, Iterable<BowtieMapping>> pair,
        Emitter<MatePairMappings> emitter) {
      MatePairMappings mateData = new MatePairMappings();
      mateData.setMateId(pair.first());
      mateData.setLibraryId("");
      mateData.setLeftMappings(new ArrayList<BowtieMapping>());
      mateData.setRightMappings(new ArrayList<BowtieMapping>());

      this.increment("Contrail", "BuildMatePairMappings-mates");

      // Keep track of the mappings associated with each suffix.
      HashMap<String, ArrayList<BowtieMapping>> suffixContigs =
          new HashMap<String, ArrayList<BowtieMapping>>();

      for (BowtieMapping mapping : pair.second()) {
        String suffix =
            ReadIdUtil.getMatePairSuffix(mapping.getReadId().toString());

        if (!suffixContigs.containsKey(suffix)) {
          suffixContigs.put(suffix, new ArrayList<BowtieMapping>());
        }

        suffixContigs.get(suffix).add(SpecificData.get().deepCopy(
           mapping.getSchema(), mapping));
      }

      // Get the links.
      if (suffixContigs.size() > 2) {
        // Error.
        this.increment(
            "Contrail-Errors", "BuildMatePairMappings-too-many-mate-pair-suffixes");
        return;
      }
      if (suffixContigs.size() == 1) {
        // Error.
        this.increment("Contrail-Errors", "BuildMatePairMappings-single-reads");
      }

      Iterator<ArrayList<BowtieMapping>> iter =
          suffixContigs.values().iterator();

      if (iter.hasNext()) {
        mateData.setLeftMappings(iter.next());
      }

      if (iter.hasNext()) {
        mateData.setRightMappings(iter.next()) ;
      } else {
        mateData.setRightMappings(new ArrayList<BowtieMapping>());
      }
      this.increment("Contrail", "BuildMatePairMappings-mate-pair-mappings");
      emitter.emit(mateData);
    }
  }

  /**
   * Key the bowtie mappings by the read id.
   */
  public static class KeyByMateIdDo extends
      DoFn<BowtieMapping, Pair<String, BowtieMapping>> {
    @Override
    public void process(
        BowtieMapping mapping,
        Emitter<Pair<String, BowtieMapping>> emitter) {
      String mateId = ReadIdUtil.getMateId(mapping.getReadId().toString());
      emitter.emit(new Pair<String, BowtieMapping>(mateId, mapping));
    }
  }

  /**
   * Return just the id of the read.
   */
  public static class ReadIdDo extends DoFn<BowtieMapping, String> {
    @Override
    public void process(BowtieMapping mapping, Emitter<String> emitter) {
      emitter.emit(mapping.getReadId().toString());
    }
  }
}
