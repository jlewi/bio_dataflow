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

package contrail.pipelines;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * An example which runs all stages required to process the Ba10K dataset.
 *
 */
public class AssembleBa10KContigs extends AssembleContigs {
  private static final Logger sLogger = Logger
      .getLogger(AssembleBa10KContigs.class);

  public AssembleBa10KContigs() {
    // Initialize the stage options to the defaults. We do this here
    // because we want to make it possible to overload them from the command
    // line.
    setDefaultParameters();
  }

  /**
   * Set the default parameters for the ba10K dataset.
   */
  @Override
  protected void setDefaultParameters() {
    // Set any parameters to the default value if they haven't already been
    // set.
    if (!stage_options.containsKey("K")) {
      stage_options.put("K", new Integer(21));
    }
    if (!stage_options.containsKey("tiplength")) {
      stage_options.put("tiplength", new Integer(50));
    }
    if (!stage_options.containsKey("bubble_edit_rate")) {
      stage_options.put("bubble_edit_rate", new Float(.05f));
    }
    if (!stage_options.containsKey("bubble_length_threshold")) {
      stage_options.put("bubble_length_threshold", new Integer(42));
    }
    if (!stage_options.containsKey("length_thresh")) {
      stage_options.put("length_thresh", new Integer(42));
    }
    if (!stage_options.containsKey("low_cov_thresh")) {
      stage_options.put("low_cov_thresh", new Float(5.0f));
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new AssembleBa10KContigs(), args);
    System.exit(res);
  }
}
