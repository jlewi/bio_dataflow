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
package contrail.stages;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import contrail.util.AvroFileUtil;
import contrail.util.ContrailLogger;
import contrail.util.FileHelper;

/**
 * Wrapper for StageInfo which provides useful helper routines.
 */
public class StageInfoHelper {
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      StageInfoHelper.class);
  private final StageInfo info;

  // Store the filename where the data was loaded from if it was loaded
  // from a file.
  private Path filePath;

  /**
   * Construct the object.
   *
   * A reference to data is stored so be careful about modifying it.
   */
  public StageInfoHelper(StageInfo data) {
    info = data;
  }

  /**
   * Construct the object.
   *
   * A reference to data is stored so be careful about modifying it.
   */
  public StageInfoHelper(StageInfo data, Path filePath) {
    info = data;
    this.filePath = filePath;
  }

  /**
   * Returns the file path from which the file was loaded or null if it
   * wasn't loaded from a file.
   * @return
   */
  public Path getFilePath() {
    return filePath;
  }

  public StageInfo getInfo() {
    return info;
  }

  /**
   * List all output paths that haven't already been deleted and aren't in
   * exclude.
   * @return
   */
  public HashSet<String> listOutputPaths(HashSet<String> exclude) {
    HashSet<String> outPaths = new HashSet<String>();
    for (StageInfo info : DFSSearch()) {
      // We only consider leaf nodes.
      // TODO(jlewi): Eventually we'd like to clean up non-leaf directories
      // as well. We could check if all the children have been deleted
      // and if they have then we can delete a non-leaf path.
      if (info.getSubStages().size() > 0) {
        continue;
      }

      boolean hasModified = false;
      for (StageParameter p : info.getModifiedParameters()) {
        String name = p.getName().toString();
        if (name.equals("outputpath")) {
          hasModified = true;
          String value = p.getValue().toString();
          if (value.length() > 0) {
            outPaths.add(value);
          }
          break;
        }
      }

      if (hasModified) {
        continue;
      }

      for (StageParameter p : info.getParameters()) {
        String name = p.getName().toString();
        if (name.equals("outputpath")) {
          String value = p.getValue().toString();
          if (value.length() > 0) {
            outPaths.add(value);
          }
          break;
        }
      }
    }

    outPaths.removeAll(exclude);

    return outPaths;
  }

  /**
   * Mark the indicated output paths as deleted.
   */
  public void markPathsAsDeleted(HashSet<String> paths) {
    for (StageInfo info : DFSSearch()) {
      boolean hasModified = false;
      for (StageParameter p : info.getModifiedParameters()) {
        String name = p.getName().toString();
        if (name.equals("outputpath")) {
          hasModified = true;
          String value = p.getValue().toString();
          if (paths.contains(value)) {
            p.setValue("");
          }
          break;
        }
      }

      if (hasModified) {
        continue;
      }

      for (StageParameter p : info.getParameters()) {
        String name = p.getName().toString();
        if (name.equals("outputpath")) {
          String value = p.getValue().toString();
          if (paths.contains(value)) {
            StageParameter newP = new StageParameter();
            newP.setName(name);
            newP.setValue("");
            info.getModifiedParameters().add(newP);
          }
          break;
        }
      }
    }
  }

  /**
   * Load the most recent stage info file in the specified directory.
   *
   * @param conf
   * @param infoGlob: Glob matching the info files.
   */
  public static StageInfoHelper loadMostRecent(
      Configuration conf, String infoGlob) {
    ArrayList<Path> stageFiles = FileHelper.matchGlobWithDefault(
        conf, infoGlob, "*.json");
    if (stageFiles.isEmpty()) {
      sLogger.info("No stage info files matched:" + infoGlob);
      return null;
    }

    // Sort the files.
    Collections.sort(stageFiles);
    Path path = stageFiles.get(stageFiles.size() - 1);
    sLogger.info("Most recent stage info file:" + path.toString());

    Schema schema = (new StageInfo()).getSchema();
    ArrayList<StageInfo> info =
        AvroFileUtil.readJsonRecords(conf, path, schema);

    if (info.size() != 1) {
      sLogger.fatal(String.format(
          "Expected exactly 1 record in the stage info file but there were " +
          "%d records.", info.size()));
    }

    return new StageInfoHelper(info.get(0), path);
  }

  /**
   * Do a depth first search starting with the last stage run.
   */
  public static class DFSIterator implements
      Iterator<StageInfo>, Iterable<StageInfo> {
    private final StageInfo info;

    // The path to the next node or null if no path.
    private ArrayList<Integer> path;

    public DFSIterator(StageInfo info) {
      this.info = info;
      path = descendMostRecent(info);
    }

    /**
     * Return the index corresponding to the most recent executed stage
     * at each step.
     * @param stage
     * @return
     */
    private ArrayList<Integer> descendMostRecent(StageInfo stage) {
      ArrayList<Integer> indexes = new ArrayList<Integer>();
      if (stage.getSubStages() == null) {
        return indexes;
      }

      // Descend until we find the most recently executed stage node.
      while(stage.getSubStages().size() > 0) {
        int index = stage.getSubStages().size() - 1;
        indexes.add(index);
        stage = stage.getSubStages().get(index);
        if (stage.getSubStages() == null) {
          break;
        }
      }
      return indexes;
    }
    @Override
    public boolean hasNext() {
      return path != null;
    }

    private ArrayList<StageInfo> getPath(ArrayList<Integer> path) {
      ArrayList<StageInfo> result = new ArrayList<StageInfo>();
      result.add(info);
      StageInfo last = info;
      for (Integer i : path) {
        last = last.getSubStages().get(i);
        result.add(last);
      }
      return result;
    }

    @Override
    public StageInfo next() {
      if (path == null) {
        throw new NoSuchElementException();
      }

      StageInfo value = null;
      if (path.size() == 0) {
        // Set path to null to indicate no more elements.
        path = null;
        // Return the root node.
        return info;
      } else {
        ArrayList<StageInfo> infoPath = getPath(path);
        // The value to return is the last one
        value = infoPath.get(infoPath.size() - 1);

        // Now find the next value to return.
        int i = path.get(path.size() - 1);
        if (i > 0) {
          // There's another stage at this level.
          --i;
          path.set(path.size() - 1 , i);

          StageInfo parent = infoPath.get(infoPath.size() - 2);
          StageInfo sibling = parent.getSubStages().get(i);

          // Descend as much as possible.
          path.addAll(descendMostRecent(sibling));
        } else {
          // Remove the last element so we process the parent.
          path.remove(path.size() - 1);
        }
      }

      return value;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<StageInfo> iterator() {
      return new DFSIterator(info);
    }
  }

  /**
   * Return an iterator which iterates over the stages in depth first order.
   * @return
   */
  public DFSIterator DFSSearch() {
    return new DFSIterator(info);
  }
}
