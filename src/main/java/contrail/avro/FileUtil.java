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
   * @param base
   * @param opath
   * @param npath
   * @throws IOException
   */
  public static void saveResult(
      JobConf conf, String base, String opath, String npath)
          throws IOException {
    FileSystem.get(conf).delete(new Path(base+npath), true);
    FileSystem.get(conf).rename(new Path(base+opath), new Path(base+npath));
  }
}
