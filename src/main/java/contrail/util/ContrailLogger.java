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
// Author: Jeremy Lewi
package contrail.util;

import org.apache.log4j.Logger;
import static org.junit.Assert.fail;

/**
 * Custom logger for Contrail.
 *
 * This is a wrapper for the standard apache logger which allows us to
 * add custom hooks. For example, it allows us to automatically abort when
 * logging a fatal message.
 */
public class ContrailLogger {
  private Logger logger;

  // Whether or not we exit automatically for a fatal log message.
  protected static boolean exitOnFatal = true;
  protected static boolean testMode = false;

  public ContrailLogger(Logger logger) {
    this.logger = logger;
  }

  public static ContrailLogger getLogger(Class cls) {
    return new ContrailLogger(Logger.getLogger(cls));
  }

  public void info(Object message) {
    logger.info(message);
  }

  public void fatal(Object message, Throwable t) {
    logger.fatal(message, t);

    if (exitOnFatal) {
      System.exit(-1);
    }

    if (testMode) {
      // Report this as a test failure.
      fail("Fatal error.");
    }
  }

  public static void setExitOnFatal(boolean value) {
    exitOnFatal = value;
  }

  public static void setTestMode(boolean value) {
    testMode = value;
  }
}
