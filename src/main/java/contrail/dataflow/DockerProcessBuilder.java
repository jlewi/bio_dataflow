/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package contrail.dataflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Build a proccess to run in a shell process.
 */
public class DockerProcessBuilder {
  private ProcessBuilder builder;
  private final List<String> command;
  private String imageName;

  private final List<VolumeMapping> volumeMappings;

  // Information about how to map a local file system path to a path in
  // the docker filesystem.
  private class VolumeMapping {
    public final String local;
    public final String container;

    public VolumeMapping(String local, String container) {
      this.local = local;
      this.container = container;
    }

    /**
     * Return a string representing the value to pass along with the -v
     * option in the docker run command.
     */
    public String toArgument() {
      return local + ":" + container;
    }
  }

  public DockerProcessBuilder(List<String> command) {
    this.command = command;

    volumeMappings = new ArrayList<VolumeMapping>();
  }

  /**
   * Add a mapping from localDir on the local filesystem to the directory
   * containerDir in the filesystem.
   *
   * @param localDir
   * @param containerDir
   */
  public void addVolumeMapping(String localDir, String containerDir) {
    volumeMappings.add(new VolumeMapping(localDir, containerDir));
  }

  public void setImage(String imageName) {
    this.imageName = imageName;
  }

  public DockerProcess start() throws IOException {
    ArrayList<String> dockerCommand = new ArrayList<String>();

    // See docker run command: https://docs.docker.com/reference/run
    // We run the command in the foreground.
    // By default this causes stdin, stdout, stderr of the command to be
    // attached to the process's stdin, stdout, stderr.
    dockerCommand.add("docker");
    dockerCommand.add("run");

    // -t causes the terminal to pretend to be a tty which is what most
    // executables expect.
    dockerCommand.add("-t");

    if (imageName == null || imageName.isEmpty()) {
      throw new IllegalArgumentException("No docker image specified.");
    }

    // Give the container a random name which we can use to refer to it
    // later on.
    Random generator = new Random();
    String name = String.format("container-%016x", generator.nextLong());
    dockerCommand.add("--name");
    dockerCommand.add(name);

    // Mount volumes.
    for (VolumeMapping m  : volumeMappings) {
      dockerCommand.add("-v");
      dockerCommand.add(m.toArgument());
    }

    dockerCommand.add(imageName);

    dockerCommand.addAll(command);

    builder = new ProcessBuilder(dockerCommand);

    return new DockerProcess(name, builder.start(), dockerCommand);
  }
}
