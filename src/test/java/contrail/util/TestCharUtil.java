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
package contrail.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;

public class TestCharUtil {
  @Test
  public void testToSet() {
    List<CharSequence> input = new ArrayList<CharSequence>();
    input.add("a");
    input.add("b");
    input.add("c");

    HashSet<String> actual = CharUtil.toStringSet(input);
    HashSet<String> expected = new HashSet<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    assertEquals(expected, actual);
  }

  @Test
  public void testToList() {
    List<CharSequence> input = new ArrayList<CharSequence>();
    input.add("a");
    input.add("b");
    input.add("c");

    List<String> actual = CharUtil.toStringList(input);
    List<String> expected = new ArrayList<String>();
    expected.add("a");
    expected.add("b");
    expected.add("c");

    assertEquals(expected, actual);
  }
}
