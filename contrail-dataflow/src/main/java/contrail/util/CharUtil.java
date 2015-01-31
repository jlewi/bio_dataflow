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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

/**
 * Some utilities for working CharSequence's.
 *
 */
public class CharUtil {
  /**
   * Convert a collection of unique CharSequence's into a set of strings.
   * @param items
   * @return
   */
  public static HashSet<String> toStringSet(Collection<CharSequence> items) {
    HashSet<String> strings = new HashSet<String>();
    String value;
    for (CharSequence item : items) {
      value = item.toString();
      if (strings.contains(value)) {
        throw new RuntimeException(
            "The collection of CharSequence's has duplicates");
      }
      strings.add(item.toString());
    }
    return strings;
  }

  /**
   * Convert a collection of CharSequence's into a list of strings.
   * @param items
   * @return
   */
  public static List<String> toStringList(Collection<CharSequence> items) {
    ArrayList<String> strings = new ArrayList<String>(items.size());
    String value;
    for (CharSequence item : items) {
      value = item.toString();
      strings.add(item.toString());
    }
    return strings;
  }
}
