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
package contrail.stages;

/**
 * Base class for stages which invoke other stages.
 */
abstract public class PipelineStage extends NonMRStage {
  // Keep track of the stage info
  protected StageInfo stageInfo;

  // The stage we are currently executing if any.
  private StageBase current;

  /**
   * Helper routine for running a child.
   *
   * Child stages should always be run using this method to ensure
   * the information for the stage is properly logged.
   *
   * @param child
   */
  protected boolean executeChild(StageBase child) {
    if (stageInfo == null) {
      // Initialize the stage info.
      stageInfo = super.getStageInfo();
    }
    stageInfo.getSubStages().add(child.getStageInfo());
    current = child;
    boolean status = child.execute();
    stageInfo.getSubStages().set(
        stageInfo.getSubStages().size() - 1, child.getStageInfo());

    current = null;
    return status;
  }

  @Override
  public StageInfo getStageInfo() {
    if (stageInfo == null) {
      // Initialize the stage info.
      stageInfo = super.getStageInfo();
    }

    // Update the state for this pipeline.
    stageInfo.setState(stageState);
    // If a stage is currently executing update the info for that stage.
    if (current != null) {
      stageInfo.getSubStages().set(
          stageInfo.getSubStages().size() - 1, current.getStageInfo());
    }

    return stageInfo;
  }
}
