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

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import contrail.sequences.ReadIdUtil.ReadIdParser;

/**
 * DoFns for processing mate pairs.
 */
public class MatePairDoFns {
  /**
   * Key mate pairs by the library id.
   *
   * The library is determined by parsing the read id. The parser is
   * specified as an argument of the constructor.
   */
  public static class KeyMatePairByLibraryId
      extends DoFn<MatePair, Pair<String, MatePair>> {
    private Class<? extends ReadIdParser> parserClass;

    transient private ReadIdParser parser;

    /**
     * Create the do fn.
     * @param parseFunction The fully qualified name of the static function
     *   which parses the string
     */
    public KeyMatePairByLibraryId(
        Class<? extends ReadIdParser> parserClass) {
      this.parserClass = parserClass;
    }

    @Override
    public void initialize() {
      try {
        parser = parserClass.newInstance();
      } catch (InstantiationException e) {
        throw new RuntimeException(
            "Couldn't create parser of type: " + parserClass.getName(), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException(
            "Couldn't create parser of type: " + parserClass.getName(), e);
      }
    }

    @Override
    public void process(
        MatePair input, Emitter<Pair<String, MatePair>> emitter) {
      ReadId leftId = parser.parse(input.getLeft().getId().toString());
      ReadId rightId = parser.parse(input.getRight().getId().toString());
      if (leftId == null || rightId == null) {
        increment("Contrail", "KeyMatePairByLibraryId-id-parsing-failed");
        return;
      }
      if (leftId.getLibrary() == null || rightId.getLibrary() == null) {
        increment("Contrail", "KeyMatePairByLibraryId-null-library");
        return;
      }
      String leftLib = leftId.getLibrary().toString();
      String rightLib = rightId.getLibrary().toString();
      if (leftLib.isEmpty() || rightLib.isEmpty()) {
        increment("Contrail", "KeyMatePairByLibraryId-empty-library");
        return;
      }
      if (!leftLib.equals(rightLib)) {
        increment("Contrail", "KeyMatePairByLibraryId-mismatch");
        return;
      }

     increment("Contrail", "KeyMatePairByLibraryId-library-" + leftLib);

     emitter.emit(new Pair(leftLib, input));
    }
  }
}
