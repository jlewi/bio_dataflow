package contrail.avro;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * Collection of routines for managing files.
 */
public class FileUtil {
  /**
   * Function which "saves" some intermediary data by renaming it.
   * @param old_path: The existing directory.
   * @param new_path: The new directory.
   * @throws IOException
   */
  public static void saveResult(
      JobConf conf, String old_path, String new_path)
          throws IOException {
    FileSystem.get(conf).delete(new Path(new_path), true);
    FileSystem.get(conf).rename(new Path(old_path), new Path(new_path));
  }
}
