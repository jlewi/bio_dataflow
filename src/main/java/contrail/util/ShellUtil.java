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
 * Some utilities for working with a shel.
 */
public class ShellUtil {
  private static int runProcess(
      ProcessBuilder builder, String prefix, String command, Logger logger) {
    try{
      logger.info("Executing command:" + command);
      Process p = builder.start();
      synchronized(p) {
        BufferedReader stdInput = new BufferedReader(
            new InputStreamReader(p.getInputStream()));

        BufferedReader stdError = new BufferedReader(
            new InputStreamReader(p.getErrorStream()));

        // In milliseconds.
        final long TIMEOUT = 1000;

        boolean wait = true;
        while (wait) {
          // We periodically check if the process has terminated and if not,
          // print out all the processes output
          p.wait(TIMEOUT);


          // Print all the output
          String line;

          while ((stdInput.ready()) && ((line = stdInput.readLine()) != null)) {
            logger.info(prefix + line);
          }
          while ((stdError.ready()) && ((line = stdError.readLine()) != null)) {
            logger.error(prefix + line);
          }
          try {
            p.exitValue();
            // Process is done.
            wait = false;
          } catch (IllegalThreadStateException e) {
            // Process hasn't completed yet.
          }
        }

        logger.info(prefix + " Exit Value: " + p.exitValue());
        if (p.exitValue() != 0) {
          logger.error(
              prefix + "command: " + command + " exited with non-zero status/");
        }
      }
      return p.exitValue();
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
   */
  public static int execute(String command, String prefix, Logger logger) {
    ProcessBuilder builder = new ProcessBuilder(command);
    return runProcess(builder, prefix, command, logger);
  }

  /**
   * Execute the command in a subprocess.
   * @param command: List of the command and its arguments.
   * @param prefix: A prefix to include in the log messages.
   * @param directory: To execute the command from
   * @param logger: Logger to log the output to.
   * @return
   */
  public static int execute(
    List<String> command, String directory, String prefix, Logger logger) {

    logger.info("Executing command: " + StringUtils.join(command, " "));
    ProcessBuilder builder = new ProcessBuilder(command);
    if ((directory != null) && (directory.length()) > 0) {
      builder.directory(new File(directory));
    }
    return runProcess(builder, prefix, StringUtils.join(command, " "), logger);
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
    try{
      logger.info("Executing command:" + command);
      Process p = builder.start();
      synchronized(p) {
        BufferedReader stdInput = new BufferedReader(
            new InputStreamReader(p.getInputStream()));

        BufferedReader stdError = new BufferedReader(
            new InputStreamReader(p.getErrorStream()));

        // In milliseconds.
        final long TIMEOUT = 1000;

        boolean wait = true;
        while (wait) {
          // We periodically check if the process has terminated and if not,
          // print out all the processes output
          p.wait(TIMEOUT);

          // Print all the output
          String line;

          while ((stdInput.ready()) && ((line = stdInput.readLine()) != null)) {
            outStream.println(line);
          }

          while ((stdError.ready()) && ((line = stdError.readLine()) != null)) {
            logger.error(prefix + line);
          }
          try {
            p.exitValue();
            // Process is done.
            wait = false;
          } catch (IllegalThreadStateException e) {
            // Process hasn't completed yet.
          }
        }

        logger.info(prefix + " Exit Value: " + p.exitValue());
        if (p.exitValue() != 0) {
          logger.error(
              prefix + "command: " + command + " exited with non-zero status/");
        }
      }
      return p.exitValue();
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
}
