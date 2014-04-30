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
import java.util.HashMap;
import java.util.Random;

import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.sequences.MatePairDoFns;
import contrail.sequences.ReadIdUtil;

public class TestMatePairDoFns {
  @Test
  public void testKeyMatePairByLibraryId() {
    HashMap<String, MatePair> expected = new HashMap<String, MatePair>();
    ArrayList<MatePair> inputPairs = new ArrayList<MatePair>();
    Random generator = new Random();

    String readFormat = "lib_%d_%d/%d";
    for (int i = 0; i < 3; ++i) {
      FastQRecord left = new FastQRecord();
      left.setId(String.format(readFormat, i, i, 1));
      left.setQvalue("");
      left.setRead(AlphabetUtil.randomString(
          generator, 10, DNAAlphabetFactory.create()));

      FastQRecord right = new FastQRecord();
      right.setId(String.format(readFormat, i, i, 2));
      right.setQvalue("");
      right.setRead(AlphabetUtil.randomString(
          generator, 10, DNAAlphabetFactory.create()));

      MatePair pair = new MatePair();
      pair.setLeft(left);
      pair.setRight(right);

      inputPairs.add(pair);
      // The library id will be everything up to the last underscore.
      expected.put("lib_" + i , pair);
    }

    Collections.shuffle(inputPairs);

    PCollection<MatePair> input =
        MemPipeline.typedCollectionOf(
            Avros.specifics(MatePair.class),
                inputPairs);

    PTable<String, MatePair> outputTable = input.parallelDo(
        new MatePairDoFns.KeyMatePairByLibraryId(
            ReadIdUtil.ReadParserUsingUnderscore.class),
            Avros.tableOf(Avros.strings(), Avros.specifics(MatePair.class)));

    Iterable<Pair<String, MatePair>> outputs = outputTable.materialize();

    int count = 0;
    for (Pair<String, MatePair> pair : outputs) {
      assertEquals(expected.get(pair.first()), pair.second());
      ++count;
    }

    assertEquals(expected.size(), count);
  }
}
