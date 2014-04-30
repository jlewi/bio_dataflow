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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Random;

import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

public class TestFastQDoFns {
  @Test
  public void testBuildMatePairs() {
    HashMap<String, MatePair> expected = new HashMap<String, MatePair>();
    ArrayList<FastQRecord> inputReads = new ArrayList<FastQRecord>();
    Random generator = new Random();
    for (int i = 0; i < 3; ++i) {
      FastQRecord left = new FastQRecord();
      left.setId("read_" + i + "/1");
      left.setQvalue("");
      left.setRead(AlphabetUtil.randomString(
          generator, 10, DNAAlphabetFactory.create()));

      FastQRecord right = new FastQRecord();
      right.setId("read_" + i + "/2");
      right.setQvalue("");
      right.setRead(AlphabetUtil.randomString(
          generator, 10, DNAAlphabetFactory.create()));

      MatePair pair = new MatePair();
      pair.setLeft(left);
      pair.setRight(right);

      inputReads.add(left);
      inputReads.add(right);

      expected.put("read_" + i , pair);
    }

    Collections.shuffle(inputReads);

    PCollection<FastQRecord> input =
        MemPipeline.typedCollectionOf(
            Avros.specifics(FastQRecord.class),
                inputReads);

    PCollection<MatePair> outputCollection = FastQDoFns.buildMatePairs(input);

    Iterable<MatePair> outputs = outputCollection.materialize();

    int count = 0;
    for (MatePair pair : outputs) {
      String mateId = ReadIdUtil.getMateId(pair.getLeft().getId().toString());
      assertEquals(expected.get(mateId), pair);
      ++count;
    }

    assertEquals(expected.size(), count);
  }
}
