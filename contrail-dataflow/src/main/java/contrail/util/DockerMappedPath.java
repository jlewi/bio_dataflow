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

import org.apache.commons.io.FilenameUtils;

/**
 * Helper class to represent the file path for a file mapped into a docker
 * container.
 */
// container.
public class DockerMappedPath {
  // Path to the file from within the container.
  public final String containerPath;
  // Path on the host.
  public final String hostPath;

  public DockerMappedPath(String hostPath, String containerPath) {
    this.containerPath = containerPath;
    this.hostPath = hostPath;
  }

  /**
   * Create an object representing a mapped file path.
   * The directory hostBase on the host should be mapped to containerBase
   * inside the contaienr. relative should then contain the relative path
   * with respect to both locations.
   *
   * @param hostBase
   * @param containerBase
   * @param relative
   */
  public static DockerMappedPath create(
      String hostBase, String containerBase, String relative) {
    return new DockerMappedPath(
        FilenameUtils.concat(hostBase, relative),
        FilenameUtils.concat(containerBase, relative));
  }
}
