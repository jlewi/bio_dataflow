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

import org.apache.log4j.Logger;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.DockerClient.LogsParameter;
import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

/**
 * A wrapper for streaming the logs from the container.
 */
public class ContainerLogStream {
  private static final Logger sLogger = Logger.getLogger(
      ContainerLogStream.class);
  private final DockerClient docker;
  private final DockerStream whichStream;
  private final String containerId;
  
  // Number of lines read so far.
  private int numLinesRead;
  
  /**
   * Which stream or streams to fetch.
   */
  public static enum DockerStream {
    STDOUT,
    STDERR,
  }
  
  public ContainerLogStream(
      DockerClient docker, DockerStream whichStream, String containerId) {
    this.docker = docker;
    this.whichStream = whichStream;
    this.containerId = containerId;
  }
  
  /**
   * Read the next batch of lines. 
   * 
   * Returns an empty array if there is currently no more output.
   * 
   * @return
   */
  public String[] readNextLines() {
    LogsParameter parameter = null;
    if (whichStream == DockerStream.STDOUT) {
      parameter = LogsParameter.STDOUT;
    } else {
      parameter = LogsParameter.STDERR;
    }
    
    LogStream logStream;
    String data = "";
    try {
      logStream = docker.logs(containerId, parameter);
      data = logStream.readFully();
    } catch (DockerException e) {
      sLogger.error("Failed to get container logs.", e);
      return new String[0];
    } catch (InterruptedException e) {
      sLogger.error("Failed to get container logs.", e);
      return new String[0];
    }
    
    
    if (data == null) {
      return new String[0];
    }
    String[] lines = data.split("\n");
    String[] newLines = new String[lines.length - numLinesRead];
    System.arraycopy(lines, numLinesRead, newLines, 0, lines.length - numLinesRead);
    numLinesRead += newLines.length;
    return newLines;
  }
}
