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
package contrail.sequences;

import org.apache.avro.specific.SpecificData;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.types.avro.Avros;

import contrail.io.FastQWritable;

/**
 * DoFns for workfing with FastQ and FastA records.
 */
public class FastQDoFns {
  /**
   * Convert a FastQWritable to FastQ record.
   */
  public static class WritableToAvroDo
      extends DoFn<FastQWritable, FastQRecord> {
    @Override
    public void process(FastQWritable input, Emitter<FastQRecord> emitter) {
      FastQRecord record = new FastQRecord();
      record.setId(input.getId());
      record.setRead(input.getDNA());
      record.setQvalue(input.getQValue());
      increment("Contrail", "WritableToAvro-Reads");
      emitter.emit(record);
    }
  }

  /**
   * Convert a FastQ avro record to test.
   */
  public static class ToTextDo
      extends DoFn<FastQRecord, String> {
    @Override
    public void process(FastQRecord record, Emitter<String> emitter) {
      emitter.emit(FastUtil.fastQRecordToString(record));
    }
  }

  /**
   * Key a FastQRecord by its mate id.
   */
  public static class KeyByMateIdDo
      extends DoFn<FastQRecord, Pair<String, FastQRecord>> {
    @Override
    public void process(
        FastQRecord record, Emitter<Pair<String, FastQRecord>> emitter) {
      String mateId = ReadIdUtil.getMateId(record.getId().toString());
      emitter.emit(new Pair(mateId, record));
    }
  }

  /**
   * Build mate pair.
   */
  public static class BuildMatePairDo
      extends DoFn<Pair<String, Iterable<FastQRecord>>, MatePair> {
    @Override
    public void process(Pair<String, Iterable<FastQRecord>> reads,
        Emitter<MatePair> emitter) {
      MatePair matePair = new MatePair();

      int count = 0;
      for (FastQRecord read : reads.second()) {
        ++count;
        // We need to make a copy because when using an MR pipeline
        // the iterator that backs reads.second might reuse the storage
        // between calls.
        FastQRecord copy = SpecificData.get().deepCopy(read.getSchema(), read);
        if (count == 1) {
          matePair.setLeft(copy);
        } else if (count == 2) {
          matePair.setRight(copy);
        }
      }

      if (matePair.getLeft().getId().toString().compareTo(
              matePair.getRight().getId().toString()) > 0) {
        // Swap them so left always the lexicographically smaller id.
        FastQRecord right = matePair.getLeft();
        matePair.setLeft(matePair.getRight());
        matePair.setRight(right);
      }

      this.increment("FastQBuildMatePair", "#-reads-" + count);
      if (count < 2) {
        return;
      }
      emitter.emit(matePair);
    }
  }

  /**
   * Form mate pairs from the reads.
   */
  public static PCollection<MatePair> buildMatePairs(
      PCollection<FastQRecord> reads) {
    // TODO(jeremy@lewi.us) Form the mate pairs.
    PTable<String, FastQRecord> pairedReads = reads.parallelDo(
        new FastQDoFns.KeyByMateIdDo(),
        Avros.tableOf(Avros.strings(), Avros.specifics(FastQRecord.class)));

    PCollection<MatePair> mates = pairedReads.groupByKey().parallelDo(
        new FastQDoFns.BuildMatePairDo(), Avros.specifics(MatePair.class));

    return mates;
  }
}
