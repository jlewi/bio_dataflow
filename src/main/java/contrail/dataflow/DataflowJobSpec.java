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

import java.util.List;

/**
 * Describes a DataflowJob to invoke.
 */
public class DataflowJobSpec {
  public String imageName;
  public List<String> jars;
  public List<String> javaArgs;
  
  // N.B. The empty constructor is required in order to use the AvroCoder.
  public DataflowJobSpec() {    
  }
  
  public DataflowJobSpec(
      List<String> javaArgs, List<String> jars, String imageName) {
    this.javaArgs = javaArgs;
    this.jars = jars;
    this.imageName = imageName;          
  }
}
