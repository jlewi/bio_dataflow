/*
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
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Some utilities for working with a shell.
 */
public class ShellUtil {
  /**
   * Run the process.
   *
   * If outstream is null then the stdout of the process is written to the
   * logger.
   *
   * @param builder
   * @param prefix
   * @param command
   * @param logger
   * @param outStream
   * @return
   */
  private static int runProcess(
      ProcessBuilder builder, String prefix, String command, Logger logger,
      PrintStream outStream) {
    try{
      logger.info("Executing command:" + command);
      Process process = builder.start();
      synchronized(process) {
        BufferedReader stdInput = new BufferedReader(
            new InputStreamReader(process.getInputStream()));

        BufferedReader stdError = new BufferedReader(
            new InputStreamReader(process.getErrorStream()));

        // In milliseconds.
        final long TIMEOUT = 1000;

        boolean wait = true;
        while (wait) {
          // We periodically check if the process has terminated and if not,
          // print out all the processes output
          process.wait(TIMEOUT);

          // Print all the output
          String line;

          while ((stdInput.ready()) && ((line = stdInput.readLine()) != null)) {
            if (outStream != null) {
              outStream.println(line);
            } else {
              logger.info(prefix + line);
            }
          }
          while ((stdError.ready()) && ((line = stdError.readLine()) != null)) {
            // TODO(jlewi): We should use logger.log and use function arguments
            // to specify what priority the output should be logged at.
            logger.error(prefix + line);
          }
          try {
            process.exitValue();
            // Process is done.
            wait = false;
          } catch (IllegalThreadStateException e) {
            // Process hasn't completed yet.
          }
        }

        logger.info(prefix + " Exit Value: " + process.exitValue());
        if (process.exitValue() != 0) {
          logger.error(
              prefix + "command: " + command + " exited with non-zero status/");
        }
      }
      return process.exitValue();
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem executing the command:\n" +
              command + "\n. The Exception was:\n" + e.getMessage());
    } catch (InterruptedException e) {
      throw new RuntimeException(
          prefix + ": execution was interupted. The command was:\n" +
              command + "\n. The Exception was:\n" + e.getMessage());
    }
  }

  /**
   * Execute the command in a subprocess.
   * @param command: Command to execute.
   * @param prefix: A prefix to include in the log messages.
   * @param logger: Logger to log the output to.
   * @return
   *
   * TODO(jeremy@lewi.us): ProcessBuilder appears to choke in many cases
   * where command contains multiple space separated arguments. I think
   * it might treat the entire string as the program name. We should try
   * to fix this by manually splitting the command and then calling one
   * of the other functions.
   */
  public static int execute(String command, String prefix, Logger logger) {
    ProcessBuilder builder = new ProcessBuilder(command);
    return runProcess(builder, prefix, command, logger, null);
  }

  /**
   * Execute the command in a subprocess.
   * @param command: List of the command and its arguments.
   * @param prefix: A prefix to include in the log messages.
   * @param directory: To execute the command from
   * @param logger: Logger to log the output to.
   * @return
   *
   */
  public static int execute(
    List<String> command, String directory, String prefix, Logger logger) {

    logger.info("Executing command: " + StringUtils.join(command, " "));
    ProcessBuilder builder = new ProcessBuilder(command);

    if ((directory == null) || (directory.length()) == 0) {
      directory = System.getProperty("user.dir");
      logger.info("Executing command in working directory: " + directory);
    } else {
      logger.info("Executing command in directory: " + directory);
    }
    builder.directory(new File(directory));

    return runProcess(
        builder, prefix, StringUtils.join(command, " "), logger, null);
  }

  /**
   * Execute the command redirecting std output to the stream.
   *
   * @return
   */
  public static int executeAndRedirect(
      List<String> command, String directory, String prefix, Logger logger,
      PrintStream outStream) {
    logger.info("Executing command: " + StringUtils.join(command, " "));
    ProcessBuilder builder = new ProcessBuilder(command);
    if ((directory != null) && (directory.length()) > 0) {
      builder.directory(new File(directory));
    }
    return runProcess(
        builder, prefix, StringUtils.join(command, " "), logger, outStream);
  }
}
