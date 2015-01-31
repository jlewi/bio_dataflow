/**
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Some routines for working with files.
 *
 * The class is named FileHelper as opposed to FileUtil to avoid confusion
 * with the FileUtil class that Apache provides for hadoop.
 */
public class FileHelper {
  private static final Logger sLogger = Logger.getLogger(FileHelper.class);

  /**
   * Create a local temporary directory.
   *
   * This function uses the system temporary directory. To control the
   * location of the system directory you can set the VM argument
   * java.io.tmpdir.
   *
   * e.g -Djava.io.tmpdir=/some/other/tmpdir
   * in which case the created directory will be a sub directory of
   * /some/other/tmpdir
   *
   * Note: When running hadoop the VM arguments for the main binary are
   * set via the environment variable HADOOP_OPTS you cannot set them on
   * the command line.
   *
   * @return
   */
  static public File createLocalTempDir() {
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HHmmss");
    Date date = new Date();
    String timestamp = formatter.format(date);

    // TODO(jlewi): Is there a java function we could use?
    File temp = null;
    try {
      temp = File.createTempFile("temp-" + timestamp + "-", "");
    } catch (IOException exception) {
      sLogger.fatal("Could not create temporary file.", exception);
      System.exit(-1);
    }

    if(!(temp.delete())){
      throw new RuntimeException(
          "Could not delete temp file: " + temp.getAbsolutePath());
    }

    if(!(temp.mkdir())) {
      throw new RuntimeException(
          "Could not create temp directory: " + temp.getAbsolutePath());
    }
    return temp;
  }

  /**
   * Function copies the contents of old_path into new_path. This is used
   * to save the final graph.
   * @param oldPath
   * @param newPath
   */
  static public void copyDirectoryContents(
      Configuration conf, String oldPath, String newPath) {
    FileSystem fs = null;
    final boolean deleteSource = false;
    // TODO(jlewi): Should we overwrite the destination if it exists?
    final boolean overwrite = false;
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
        FileUtil.copy(fs, oldFile, fs, newFile, deleteSource, overwrite, conf);
      }
    } catch (IOException e) {
      throw new RuntimeException("Problem copying the files: " + e.getMessage());
    }
  }

  /**
   * Function moves the contents of old_path into new_path. This is used
   * to save the final graph.
   * @param oldPath
   * @param newPath
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

  /**
   * Find files matching the glob expression.
   *
   * This only works for the local/non hadoop filesystem.
   * @param glob
   * @return
   */
  public static ArrayList<String> matchFiles(String glob) {
    // We assume glob is a directory + a wild card expression
    // e.g /some/dir/*.fastq
    File dir = new File(FilenameUtils.getFullPath(glob));
    String pattern = FilenameUtils.getName(glob);
    FileFilter fileFilter = new WildcardFileFilter(pattern);

    File[] files =  dir.listFiles(fileFilter);
    ArrayList<String> result = new ArrayList<String>();

    if (files == null || files.length == 0) {
      return result;
    }

    for (File file : files) {
      result.add(file.getPath());
    }
    return result;
  }

  /**
   * Find files matching a list of globular expressions.
   *
   * This only works for the local/non hadoop filesystem
   * .
   * @param glob
   * @return
   */
  public static ArrayList<String> matchListOfGlobs(String globs) {
    HashSet<String> matches = new HashSet<String>();
    for (String glob : globs.split(",")) {
      ArrayList<String> newMatches =  FileHelper.matchFiles(glob);
      sLogger.info(String.format("%s matched %d files", glob,
          newMatches.size()));
      matches.addAll(newMatches);
    }
    ArrayList<String> result = new ArrayList<String>();
    result.addAll(matches);
    return result;
  }

  /**
   * Return a list of files matching a globular expression.
   *
   * If globOrDirectory points to a directory then we return all files matching
   * globOrDirectory + defaultGlob.
   *
   * If globOrDirectory is not a directory then we treat it as a glob path.
   * @param conf: Hadoop configuration.
   * @param globOrDirectory: A directory or a globular expression.
   * @param defaultGlob: The default file glob.
   * @return
   */
  public static ArrayList<Path> matchGlobWithDefault(
      Configuration conf, String globOrDirectory,  String defaultGlob) {
    Path globOrDirectoryPath = new Path(globOrDirectory);
    FileSystem fs = null;
    if (conf == null) {
      sLogger.fatal("conf cannot be null.",
          new IllegalArgumentException("conf is null."));
      System.exit(-1);
    }

    try{
      fs = globOrDirectoryPath.getFileSystem(conf);
    } catch (IOException e) {
      throw new RuntimeException("Can't get filesystem: " + e.getMessage());
    }

    try {
      if (fs.exists(globOrDirectoryPath)) {
          if (fs.getFileStatus(globOrDirectoryPath).isDir()) {
            String pattern = FilenameUtils.concat(
                globOrDirectory, defaultGlob);
            sLogger.info(String.format(
                "Path:%s is an existing directory.\n Look for files " +
                "matching glob:%s", globOrDirectoryPath, pattern));
            globOrDirectoryPath = new Path(pattern);
          } else {
            sLogger.info(String.format(
                "Path:%s is an existing file.", globOrDirectoryPath));
            ArrayList<Path> paths = new ArrayList<Path>();
            paths.add(globOrDirectoryPath);
            return paths;
          }
      } else {
        sLogger.info(String.format(
            "Path:%s is a glob expression.", globOrDirectoryPath));
      }
    } catch(IOException e) {
      throw new RuntimeException(e);
    }

    try {
      ArrayList<Path> paths = new ArrayList<Path>();
      // globStatus returns null if path doesn't exist.
      FileStatus[] statuses = fs.globStatus(globOrDirectoryPath);
      if (statuses == null) {
        return paths;
      }
      for (FileStatus status : statuses) {
        paths.add(status.getPath());
      }
      return paths;
    } catch (IOException e) {
      throw new RuntimeException("Problem moving the files: " + e.getMessage());
    }
  }

  /**
   * Find all the files matching a comma separated list of globs.
   *
   * The result can contain duplicates if a file matches more than one
   * expression.
   *
   * @param conf
   * @param listOfGlobs: a comma separated string of directories or globs.
   * @param defaultGlob
   * @return
   */
  public static ArrayList<Path> matchListOfGlobsWithDefault(
      Configuration conf, String listOfGlobs,  String defaultGlob) {
    ArrayList<Path> results = new ArrayList<Path>();
    String[] globs = listOfGlobs.split(",");
    for (String glob : globs) {
      results.addAll(matchGlobWithDefault(conf, glob, defaultGlob));
    }
    return results;
  }
}
