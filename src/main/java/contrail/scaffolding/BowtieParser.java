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

import org.apache.log4j.Logger;

/**
 * A class for converting the output of bowtie into an avro record.
 *
 * Bowtie output is described at:
 * http://bowtie-bio.sourceforge.net/manual.shtml#default-bowtie-output
 */
public class BowtieParser {
  private static final Logger sLogger = Logger.getLogger(BowtieParser.class);

  /**
   * Parse the contents of line into mapping.
   * @param line
   * @param mapping
   */
  public void parse(String line, BowtieMapping mapping) {
    // For the description of the bowtie output see:
    // http://bowtie-bio.sourceforge.net/manual.shtml#default-bowtie-output
    //
    // Split the string based on whitespace. "\s" matches a whitespace
    // character and the + modifier causes it to be matched one or more
    // times. An extra "\" is needed to escape the "\s".
    // For more info:
    // http://docs.oracle.com/javase/tutorial/essential/regex/
    String[] splitLine = line.trim().split("\\s+");

    // The line should have at least 5 fields.
    if (splitLine.length <5) {
      sLogger.fatal(
          "Line in the bowtie output file had less than 5 fields:" + line,
          new RuntimeException("Parse Error"));
      System.exit(-1);
    }

    // The first field in the output is the name of the read that was
    // aligned. The second field is a + or - indicating which strand
    // the read aligned to.
    String readID = splitLine[0];
    String strand = splitLine[1];
    String contigID = splitLine[2];
    // 0-based offset into the forward reference strand where leftmost
    // character of the alignment occurs.
    String forwardOffset = splitLine[3];
    String readSequence = splitLine[4];

    int numMismatches = 0;
    if (splitLine.length > 7) {
      numMismatches = splitLine[7].split(",").length;
    }

    mapping.setContigId(contigID);
    mapping.setNumMismatches(numMismatches);

    // isFWD indicates the read was aligned to the forward strand of
    // the reference genome.
    Boolean isFwd = null;
    if (strand.equals("-")) {
      isFwd = false;
    } else if (strand.equals("+")) {
      isFwd = true;
    } else {
      throw new RuntimeException("Couldn't parse the alignment strand");
    }

    // TODO(jeremy@lewi.us): It looks like the original code prefixed
    // the read id's with the library name.
    // We don't prefix the read with the library
    // name because we think that's more likely to cause problems because
    // the read ids would need to be changed consistently everywhere.
    mapping.setReadId(readID);

    int start = Integer.parseInt(forwardOffset);

    // The range is inclusive; i.e the relevant subsequence is [start, end].
    // We abandon the usual java semantics because the start and end indexes
    // get reversed depending on whether the read aligns to the forward
    // or reverse strand of the contig. If the start index was included
    // but not the end index, then reversing the indexes would be a lot
    // more complicated.
    int end = start + readSequence.length() - 1;

    if (isFwd) {
      mapping.setContigStart(start);
      mapping.setContigEnd(end);
    } else {
      mapping.setContigStart(end);
      mapping.setContigEnd(start);
    }

    // The clear range for the read is just the start and end indexes.
    // The indexes are inclusive.
    mapping.setReadClearStart(0);
    mapping.setReadClearEnd(readSequence.length() -1);

    // TODO(jeremy@lewi.us): Add a parameter to control whether the
    // read is included or not.
    // Note: If the read aligned to the reverse strand (isFwd is False)
    // then bowtie sets readSequence to the reverse complement of the
    // actual read.
    mapping.setRead(readSequence);
  }
}
