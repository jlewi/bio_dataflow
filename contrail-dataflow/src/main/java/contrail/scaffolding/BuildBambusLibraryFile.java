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
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import contrail.io.AvroFileContentsIterator;
import contrail.sequences.MatePair;
import contrail.stages.NonMRStage;
import contrail.stages.ParameterDefinition;
import contrail.util.AvroFileUtil;
import contrail.util.ContrailLogger;

/**
 * Writer for the library file.
 *
 * The library file lists each mate pair in each library.
 *
 * For more info on the bambus format see:
 * http://www.cs.jhu.edu/~genomics/Bambus/Manual.html#matesfile
 */
public class BuildBambusLibraryFile extends NonMRStage {
  private static final ContrailLogger sLogger =
      ContrailLogger.getLogger(BuildBambusLibraryFile.class);

  private Path outPath;
  private FSDataOutputStream outStream;

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

    for (ParameterDefinition def:
      new ParameterDefinition[] {libFile, outputPath}) {
      definitions.put(def.getName(), def);
    }

    return Collections.unmodifiableMap(definitions);
  }

  /** Generate a safe name for bambus. */
  private static String safeLibraryName(String name) {
      return name.replaceAll("_", "");
  }

  /**
   * Parse the library file and extract the library sizes.
   */
  private List<Library> loadLibraries(String libFile) {
    List<Library> libraries = AvroFileUtil.readJsonArray(
        getConf(), new Path(libFile), Library.SCHEMA$);

    for (Library lib : libraries) {
      // Make the name safe for bambus.
      lib.setName(safeLibraryName(lib.getName().toString()));
    }
    return libraries;
  }


  /** Write to the output stream. */
  private void write(String line) {
    try {
      outStream.write(line.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      sLogger.fatal("Could not write output.", e);
    } catch (IOException e) {
      sLogger.fatal("Could not write output.", e);
    }
  }
  /**
   * Write the name of a library and its min and max insert size.
   */
  private void writeLibrary(Library lib) {
    write(
        "library " + lib.getName() + " " + lib.getMinSize() + " " +
        lib.getMaxSize() + "\n");
  }

  /**
   * Write the ids of a mate pair.
   */
  public void writeMateIds(String leftId, String rightId, String libName) {
    // If the left read name starts with "l", "p" or "#" we have a problem
    // because  the binary toAmos_new in the amos package reserves uses these
    // characters to identify special types of rows in the file.
    if (leftId.startsWith("l") || leftId.startsWith("p") ||
        leftId.startsWith("#")) {
      sLogger.fatal(
          "The read named:" + leftId + " will cause problems with the " +
          "amos binary toAmos_new. The amos binary attributes special " +
          "meaning to rows in the library file starting with 'p', 'l' or " +
          "'#' so if the id for a read starts with any of those " +
          "characters it will mess up amos.",
          new RuntimeException("Invalid read name"));
      System.exit(-1);
    }
    write(leftId + " " + rightId + " " + libName + "\n");
  }

  public void close() {
    try {
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal(
          "Could not close the path: " + outPath.toString(), e);
    }
  }

  @Override
  protected void stageMain() {
    String libFile = (String) stage_options.get("library_file");

    Path outPath = new Path((String) stage_options.get("outputpath"));
    FileSystem fs;
    try {
      fs = outPath.getFileSystem(getConf());
      outStream = fs.create(outPath, true);
    } catch (IOException e) {
      sLogger.fatal("Could not open the output path for writing. path:" +
                    outPath.toString(), e);
    }

    List<Library> libraries = loadLibraries(libFile);

    for (Library lib : libraries) {
      writeLibrary(lib);

      ArrayList<String> pairFiles = new ArrayList<String>();
      for (CharSequence mateFile : lib.getFiles()) {
        pairFiles.add(mateFile.toString());
      }

      AvroFileContentsIterator<MatePair> pairIterator =
          new AvroFileContentsIterator<MatePair>(pairFiles, getConf());

      for (MatePair pair : pairIterator) {
        writeMateIds(
            pair.getLeft().getId().toString(),
            pair.getRight().getId().toString(),
            lib.getName().toString());
      }
    }

    close();
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new BuildBambusLibraryFile(), args);
    System.exit(res);
  }
}
