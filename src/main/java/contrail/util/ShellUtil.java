package contrail.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

/**
 * Some utilities for working with a shel.
 */
public class ShellUtil {
  /**
   * Execute the command in a subprocess.
   * @param command: Command to execute.
   * @param prefix: A prefix to include in the log messages.
   * @param logger: Logger to log the output to.
   * @return
   */
  public static int execute(String command, String prefix, Logger logger) {
    try {
      logger.info("Executing command:" + command);
      Process p = Runtime.getRuntime().exec(command);

      BufferedReader stdInput = new BufferedReader(
          new InputStreamReader(p.getInputStream()));

      BufferedReader stdError = new BufferedReader(
          new InputStreamReader(p.getErrorStream()));

      p.waitFor();
      String line;
      while ((line = stdInput.readLine()) != null) {
        logger.info(prefix + line);
      }
      while ((line = stdError.readLine()) != null) {
        logger.error(prefix + line);
      }

      logger.info(prefix + " Exit Value: " + p.exitValue());
      if (p.exitValue() != 0) {
        logger.error(
            prefix + "command: " + command + " exited with non-zero status/");
      }
      return p.exitValue();
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem executing bowtie. The command was:\n" +
              command + "\n. The Exception was:\n" + e.getMessage());
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Bowtie execution was interupted. The command was:\n" +
              command + "\n. The Exception was:\n" + e.getMessage());
    }
  }
}
