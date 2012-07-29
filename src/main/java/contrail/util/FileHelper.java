package contrail.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Some routines for working with files.
 *
 * The class is named FileHelper as opposed to FileUtil to avoid confusion
 * with the FileUtil class that Apache provides for hadoop.
 */
public class FileHelper {
  /**
   * Function moves the contents of old_path into new_path. This is used
   * to save the final graph.
   * @param old_path
   * @param new_path
   */
  static public void moveDirectoryContents(
      Configuration conf, String oldPath, String newPath) {
    // We can't invoke rename directly on old path because it ends up
    // making old_path a subdirectory of new_path.
    FileSystem fs = null;
    try{
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }
    try {
      Path oldPathObject = new Path(oldPath);
      for (FileStatus status : fs.listStatus(oldPathObject)) {
        Path oldFile = status.getPath();
        Path newFile = new Path(newPath, oldFile.getName());
        fs.rename(oldFile, newFile);
      }
    } catch (IOException e) {
      throw new RuntimeException("Problem moving the files: " + e.getMessage());
    }
  }
}
