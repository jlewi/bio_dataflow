/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.sequences;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities for parsing and manipulating the ids of reads.
 */
public class ReadIdUtil {
  /**
   * The suffix pattern for a mate pair matches the part of a mate pair that
   * tells which mate in a pair it is.
   */
  public final static Pattern MATE_PAIR_SUFFIX_PATTERN =
      Pattern.compile("[_/][12]$");

  /**
   * Checks if the id forms a valid mate pair id.
   *
   * @param id
   */
  public static boolean isValidMateId(String id) {
    Matcher suffix = MATE_PAIR_SUFFIX_PATTERN.matcher(id);
    if(!suffix.find()) {
      // There is no mate id suffix.
      return false;
    }
    if (suffix.start() == 0) {
      // The string only consists of the suffix.
      return false;
    }
    return true;
  }

  /**
   * Returns the mate pair id.
   *
   * The mate id is the part of the read id that is common to both mates
   * in the pair. If the id isn't a valid mate id then we just return the
   * id.
   *
   * @param id
   */
  public static String getMateId(String id) {
    Matcher suffix = MATE_PAIR_SUFFIX_PATTERN.matcher(id);
    if(!suffix.find()) {
      // There is no mate id suffix.
      return id;
    }
    if (suffix.start() == 0) {
      // The string only consists of the suffix.
      throw new IllegalArgumentException("Not a valid id:" + id);
    }
    return id.substring(0, suffix.start());
  }

  /**
   * Returns the mate pair suffix or "" if not matched.
   */
  public static String getMatePairSuffix(String id) {
    Matcher suffix = MATE_PAIR_SUFFIX_PATTERN.matcher(id);
    if(!suffix.find()) {
      // There is no mate id suffix.
      return "";
    }
    return id.substring(suffix.start() + 1);
  }

  /**
   * Checks if two reads form a mate pair.
   *
   * The expectation is the two reads should be the same except for the
   * suffixes "/1" and "/2".
   *
   * @param leftId
   * @param rightId
   * @return
   */
  public static boolean isMatePair(String leftId, String rightId) {
    if (!isValidMateId(leftId)) {
      return false;
    }
    if (!isValidMateId(rightId)) {
      return false;
    }

    // Make sure the prefixes of both reads matches.
    String leftPrefix = leftId.substring(0, leftId.length() - 2);
    String rightPrefix = rightId.substring(0, rightId.length() - 2);

    if (!leftPrefix.equals(rightPrefix)) {
      return false;
    }

    // Check that the mate ids are different.
    if (leftId.charAt(leftId.length() - 1) ==
        rightId.charAt(rightId.length() - 1)) {
      return false;
    }
    return true;
  }

  private final static Pattern READ_ID_WITH_UNDERSCORE_PATTERN =
      Pattern.compile("(.*)_(.*)[_/]([12])");

  /**
   * Interface for parsing read ids.
   */
  public static interface ReadIdParser {
    /**
     * Parse the read. Return null if it couldn't be parsed.
     * @param id
     * @return
     */
    public ReadId parse(String id);
  }


  /**
   * Split a read id into component parts.
   *
   * This function uses the regular expression "(.*)_(.*)[_/]([12])"
   * Where the groupings contain the library read id and mate suffix
   * respectively.
   *
   * Returns null if it doesn't match.
   */
  public static class ReadParserUsingUnderscore implements ReadIdParser {
    @Override
    public ReadId parse(String readId) {
      Matcher m = READ_ID_WITH_UNDERSCORE_PATTERN.matcher(readId);
      if (m.matches()) {
        ReadId id = new ReadId();
        id.setLibrary(m.group(1));
        id.setId(m.group(2));
        if (m.group(3).equals("1")) {
          id.setMateId(MatePairId.LEFT);
        } else {
          id.setMateId(MatePairId.RIGHT);
        }
        return id;
      } else {
        return null;
      }
    }
  }
}
