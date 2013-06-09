/* Licensed under the Apache License, Version 2.0 (the "License");
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

/**
 * A wrapper for a json description of a BigQuery field.
 */
public class BigQueryField {
  public String name;
  public String type;
  public String mode;

  public BigQueryField(String name, String type) {
    this.name = name;
    this.type = type;
    fields = new ArrayList<BigQueryField>();
  }

  public BigQueryField() {
    fields = new ArrayList<BigQueryField>();
  }

  public ArrayList<BigQueryField> fields;

  protected static Set<String> reserved;

  public static Set<String> getReserevedKeywords() {
    if (reserved == null) {
      reserved = new HashSet<String>();
      reserved.addAll(Arrays.asList(
          "float", "integer", "boolean", "string", "repeated",
          "record"));
      reserved = Collections.unmodifiableSet(reserved);
    }
    return reserved;
  }

  public String toString() {
    // Make sure name isn't a reserved field.
    if (BigQueryField.getReserevedKeywords().contains(name.toLowerCase())) {
      throw new RuntimeException("Name: " + name + " is a reserved keyword.");
    }
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    ArrayList<String> pairs = new ArrayList<String>();

    pairs.add(String.format("\"name\":\"%s\"", name));
    pairs.add(String.format("\"type\":\"%s\"", type));

    if (mode != null) {
      pairs.add(String.format("\"mode\":\"%s\"", mode));
    }

    ArrayList<String> subFields = new ArrayList<String>();
    for (BigQueryField subField : fields) {
      subFields.add(subField.toString());
    }

    if (subFields.size() > 0) {
      pairs.add(String.format(
          "\"fields\":[%s]", StringUtils.join(subFields, ",")));
    }

    builder.append(StringUtils.join(pairs, ","));
    builder.append("}");
    return builder.toString();
  }
}
