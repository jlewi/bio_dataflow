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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.io.AvroFileContentsIterator;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastUtil;
import contrail.sequences.FastaRecord;
import contrail.sequences.MatePair;
import contrail.sequences.ReadIdUtil;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.AvroFileUtil;

/**
 * This class constructs the fasta file needed to run Bambus for scaffolding.
 *
 * The output is a single fasta file containing all the original reads. The
 * reads are shortened to the same length as we did for bowtie. This ensures
 * the mapping coordinates match up.
 *
 * The input is a json file describing each library. Each library contains
 * a list of the fastq files in that library.
 */
public class BuildBambusFastaFile extends NonMRStage {
  private static final Logger sLogger =
      Logger.getLogger(BuildBambusFastaFile.class);

  /**
   * Get the parameters used by this stage.
   */
  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> definitions =
        new HashMap<String, ParameterDefinition>();

    definitions.putAll(super.createParameterDefinitions());

    ParameterDefinition libFile =
        new ParameterDefinition(
            "library_file", "A path to the json file describing the " +
            "libraries. The json file should be an array of Library " +
            "records. The paths of the reads can be URI's to indicate the " +
            "filesystem they are on.",
            String.class, null);

    ParameterDefinition outputPath =
        new ParameterDefinition(
            "outputpath",
            "The path to write the mates file to. This will be on the " +
            "default filesystem unless the path contains a filesystem prefix.",
            String.class, null);

    ParameterDefinition readLength =
        new ParameterDefinition(
            "read_length",
            "How short to make the reads. The value needs to be consistent " +
            "with the value used in AlignReadsWithBowtie. Bowtie requires " +
            "short reads. Bambus needs to use the same read lengths as those " +
            "used by bowtie because otherwise there could be issues with " +
            "contig distances because read start/end coordinates for the " +
            "alignments aren't consistent.",
            Integer.class, 25);

    for (ParameterDefinition def:
      new ParameterDefinition[] {libFile, outputPath, readLength}) {
      definitions.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(definitions);
  }

  /**
   * We shorten the reads and write them to the fasta file.
   * We need to put all the reads into one file because toAmos_new expects that.
   *
   * The reads are truncated because BOWTIE is a short read aligner and only
   * works with short reads. Therefore, Bambus needs to use the shortened reads
   * otherwise the alignment coordinates reported by Bambus won't be consistent.
   * The reads are written to fastaOutputFile.
   *
   * @param matePairs: A collection of file pairs representing mate pair
   *    libraries.
   * @param fastaOutputFile: The file to write the shortened reads to.
   * @param libraryOutputFile: The file to write the library information to.
   */
  protected void createFastaFile(
      Collection<Library> libraries, String fastaOutputFile) {
    int readLength = (Integer) stage_options.get("read_length");

    FSDataOutputStream fastaStream = null;

    Path outPath = new Path((String) stage_options.get("outputpath"));
    FileSystem fs;
    try {
      fs = outPath.getFileSystem(getConf());
      fastaStream = fs.create(outPath, true);
    } catch (IOException e) {
      sLogger.fatal("Could not open the output path for writing. path:" +
                    outPath.toString(), e);
    }

    int counter = 0;

    for (Library lib : libraries) {
      ArrayList<String> pairFiles = new ArrayList<String>();
      for (CharSequence mateFile : lib.getFiles()) {
        pairFiles.add(mateFile.toString());
      }

      AvroFileContentsIterator<MatePair> pairIterator =
          new AvroFileContentsIterator<MatePair>(pairFiles, getConf());

      FastaRecord fasta = new FastaRecord();

      for (MatePair pair : pairIterator) {
        String leftId = pair.getLeft().getId().toString();
        String rightId = pair.getRight().getId().toString();
        if (!ReadIdUtil.isMatePair(leftId, rightId)) {
          sLogger.fatal(String.format(
              "Expecting a mate pair but the read ids: %s, %s do not form " +
                  "a valid mate pair.", leftId, rightId));
          System.exit(-1);
        }

        for (FastQRecord fastq : new FastQRecord[] {
                pair.getLeft(), pair.getRight()}) {
          fasta.setId(fastq.getId());

          // Truncate the read because bowtie can only handle short reads.
          fasta.setRead(fastq.getRead().subSequence(0, readLength));

          try {
            ++counter;
            FastUtil.writeFastARecord(fastaStream, fasta);
          } catch (IOException e) {
            sLogger.fatal("Could not write to: " + fastaOutputFile, e);
          }
        }

        if (counter % 1000000 == 0) {
          sLogger.info("Processed " + counter + " reads");
          try {
            fastaStream.flush();
          } catch (IOException e) {
            sLogger.fatal("Could not flush: " + fastaOutputFile, e);
          }
        }
      }

    }

    try {
      fastaStream.close();
    } catch (IOException e) {
      sLogger.fatal("Could not clode: " + fastaOutputFile, e);
    }
    sLogger.info("Total # of reads written: " + counter);
  }

  /**
   * Align the contigs to the reads.
   *
   * @param args
   * @throws Exception
   */
  @Override
  protected void stageMain() {
    String libFile = (String) this.stage_options.get("library_file");
    List<Library> libraries = AvroFileUtil.readJsonArray(
        getConf(), new Path(libFile), Library.SCHEMA$);

    String fastaOutputFile = (String) stage_options.get("outputpath");
    createFastaFile(libraries, fastaOutputFile);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new BuildBambusFastaFile(), args);
    System.exit(res);
  }
}
