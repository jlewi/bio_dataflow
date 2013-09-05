/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
// Author: Jeremy Lewi(jeremy@lewi.us)
package contrail.stages;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import contrail.io.AvroFileContentsIterator;
import contrail.util.CharUtil;
import contrail.util.ContrailLogger;

/**
 * Select a subset of the threadable node groupings for processing.
 *
 * SelectThreadableGraph splits the graph into subgroups consisting of
 * threadable nodes and their neighbors. We need to select a subset of groups
 * such that each node appears in at most one of these groups.
 */
public class SelectThreadableGroups extends NonMRStage{
  // Total number of groups.
  private int numGroups ;
  private int numMergeTooLarge;

  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      SelectThreadableGroups.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }

    ParameterDefinition maxGroupSize = new ParameterDefinition(
        "max_subgraph_size", "The maximum number of nodes in any of the " +
        "subgraphs.", Integer.class, new Integer(1000));

    ParameterDefinition maxHeapUsage = new ParameterDefinition(
        "max_heap_usage", "When the heap utilization exceeds this threshold  " +
        "we will stop selecting threadable group. This should be expressed " +
        "a percentage 0% - 100%.", Integer.class, new Integer(90));

    defs.put(maxGroupSize.getName(), maxGroupSize);
    defs.put(maxHeapUsage.getName(), maxHeapUsage);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * The output scheme is a pair containing the id for a subgraph and a list of
   * nodes in the subgraph
   */
  private Schema getSchema() {
    return Pair.getPairSchema(
        Schema.create(Schema.Type.STRING),
        Schema.createArray(Schema.create(Schema.Type.STRING)));
  }

  public Path getOutPath() {
    // Writer for the connected components.
    Path outDir = new Path((String)stage_options.get("outputpath"));
    Path outPath = new Path(FilenameUtils.concat(
        outDir.toString(), "threadable_subgraphs.avro"));
    return outPath;
  }

  /**
   * Create a writer. The output scheme is a pair containing the id for
   * a subgraph and a list of nodes in the subgraph.
   */
  private DataFileWriter<Pair<CharSequence, List<CharSequence>>>
      createWriter() {
    DataFileWriter<Pair<CharSequence, List<CharSequence>>> writer = null;
    Path outDir = new Path((String)stage_options.get("outputpath"));
    try {
      FileSystem fs = outDir.getFileSystem(getConf());

      if (fs.exists(outDir) && !fs.isDirectory(outDir)) {
        sLogger.fatal(
            "outputpath points to an existing file but it should be a " +
            "directory:" + outDir.getName());
        System.exit(-1);
      } else {
        fs.mkdirs(outDir, FsPermission.getDefault());
      }

      FSDataOutputStream outStream = fs.create(getOutPath(), true);

      Schema schema = getSchema();
      DatumWriter<Pair<CharSequence, List<CharSequence>>> datumWriter =
          new SpecificDatumWriter<Pair<CharSequence, List<CharSequence>>>(
              schema);
      writer =
          new DataFileWriter<Pair<CharSequence, List<CharSequence>>>(
              datumWriter);
      writer.create(schema, outStream);

    } catch (IOException exception) {
      sLogger.fatal(
          "There was a problem writing the components to an avro file. " +
           "Exception: " + exception.getMessage(), exception);
    }
    return writer;
  }

  private boolean isSorted(List<String> strings) {
    for (int i = 1; i < strings.size(); ++i) {
      // Check the strings are strictly decreasing.
      if (strings.get(i - 1).compareTo(strings.get(i)) >= 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return true if we are almost out of memory and the caller shouldn't
   * continue. As part of this process we try to free memory by running
   * garbage collection.
   */
  protected boolean almostOutOfMemory(double maxHeapUtilization) {
    Runtime runtime = Runtime.getRuntime();
    DecimalFormat sf = new DecimalFormat("00");

    // Compute memory usage.
    double utilization =
        ((double)runtime.totalMemory()) / ((double)runtime.maxMemory()) * 100;

    if (utilization < maxHeapUtilization) {
      return false;
    }

    sLogger.info(String.format(
        "Memory utilization: Total Memory: %d " +
        "Max Memory:%d Utilization %s%%", runtime.totalMemory(),
        runtime.maxMemory(), sf.format(utilization)));
    // Lets try running garbage collection to free up some memory.
    runtime.gc();

    utilization =
        ((double)runtime.totalMemory()) /
        ((double)runtime.maxMemory()) * 100;
    sLogger.info(String.format(
        "After garbage collection memory usage:" +
        "Total Memory: %d " +
        "Max Memory:%d Utilization %s%%", runtime.totalMemory(),
        runtime.maxMemory(), sf.format(utilization)));

    if (utilization < (maxHeapUtilization - 10)) {
      sLogger.info(
          "Garbage collection freed up enough memory to continue with " +
          "threadable subgraph selection.");
      return false;
    }

    sLogger.info(
        "Garbage collection did not free up enough memory to continue " +
        "with threadable subgraph selection.");
    return true;

  }

   /** Group the threadable nodes into subgraphs.
   *
   * @return: A list of subgraphs.
   */
  protected Collection<List<String>> selectSubGraphs(
      Iterable<List<CharSequence>> threadableGroups) {
    // The groupId used to indicate a node isn't assigned a group.
    final Integer unassignedGroup = -1;
    int maxGroupSize = (Integer)stage_options.get("max_subgraph_size");

    numGroups = 0;
    numMergeTooLarge = 0;
    // Mapping from a nodeId to the id for the group it is currently assigned
    // to.
    HashMap<String, Integer> idToGroup = new HashMap<String, Integer>();

    // Mapping from the id of each group to the nodes in that group.
    HashMap<Integer, List<String>> subGraphs =
        new HashMap<Integer, List<String>>();

    HashSet<Integer> groupsToMerge = new HashSet<Integer>();
    ArrayList<String> unassignedNodes = new ArrayList<String>();

    Integer maxHeapUtilization =
        (Integer) stage_options.get("max_heap_usage");

    for (List<CharSequence> group : threadableGroups) {
      ++numGroups;
      groupsToMerge.clear();
      unassignedNodes.clear();

      // TODO(jeremy@lewi.us): The group should already be sorted and not
      // contain duplicates.
      List<String> thisGroup = CharUtil.toStringList(group);
      if (!isSorted(thisGroup)) {
        sLogger.fatal(
            "Nodes appear multiple times in the input group:" +
                StringUtils.join(thisGroup,","),
            new RuntimeException("Invalid Input"));
      }

      for (String id : thisGroup) {
        Integer assignedGroup = idToGroup.get(id);
        if (assignedGroup != null && assignedGroup != unassignedGroup) {
          groupsToMerge.add(idToGroup.get(id));
        } else {
          unassignedNodes.add(id);
        }
      }

      Integer groupId = null;
      if (groupsToMerge.size() == 0) {
        // This group is unique.
        groupId = numGroups;
        subGraphs.put(groupId, thisGroup);

        for (String id : thisGroup) {
          idToGroup.put(id, groupId);
        }
        continue;
      }

      // Check if merging the groups would produce a group that is too large.
      int newSize = unassignedNodes.size();
      for (Integer mergeId : groupsToMerge) {
        newSize += subGraphs.get(mergeId).size();
      }

      if (newSize > maxGroupSize) {
        ++numMergeTooLarge;
        // Since we can't merge the groups, any unassigned nodes
        // should just be added to the unassigned group.
        for (String id : unassignedNodes) {
          idToGroup.put(id, unassignedGroup);
        }
        continue;
      }

      // Merge all the groups.
      Iterator<Integer> groupIterator = groupsToMerge.iterator();
      groupId = groupIterator.next();

      while (groupIterator.hasNext()) {
        Integer otherGroup = groupIterator.next();
        List<String> otherNodes = subGraphs.remove(otherGroup);

        subGraphs.get(groupId).addAll(otherNodes);

        for (String node : otherNodes) {
          idToGroup.put(node, groupId);
        }
      }

      // Add in the new nodes.
      subGraphs.get(groupId).addAll(unassignedNodes);
      for (String node : unassignedNodes) {
        idToGroup.put(node,  groupId);
      }

      // Check if we are almost out of memory.
      if (almostOutOfMemory(maxHeapUtilization.doubleValue())) {
        sLogger.info(
            "Halting selection of threadable subgraphs; almost out of memory.");
        break;
      }
    }

    return subGraphs.values();
  }

  @Override
  protected void stageMain() {
    String inputPath = (String) stage_options.get("inputpath");

    AvroFileContentsIterator<List<CharSequence>> threadableGroups =
        AvroFileContentsIterator.fromGlob(getConf(), inputPath);

    if (!threadableGroups.hasNext()) {
      sLogger.fatal("Input is empty.", new RuntimeException("No input."));
    }

    Collection<List<String>> subGraphs = selectSubGraphs(threadableGroups);

    Pair<CharSequence, List<CharSequence>> outPair =
        new Pair<CharSequence, List<CharSequence>>(getSchema());
    DataFileWriter<Pair<CharSequence, List<CharSequence>>> writer =
        createWriter();
    outPair.value(new ArrayList<CharSequence>());

    int graphId = -1;
    // Number of nodes in the subgraphs.
    int numNodesInSubGraphs = 0;
    for (List<String> subGraph : subGraphs) {
      Collections.sort(subGraph);
      // Make sure the list is unique.
      if (!isSorted(subGraph)) {
        sLogger.fatal(
            "The subgraph isn't sorted or contains duplicates:" +
                StringUtils.join(subGraph, ","),
            new RuntimeException("Invalid output"));
      }
      ++graphId;
      numNodesInSubGraphs += subGraph.size();
      outPair.key(String.format("%03d", graphId));

      if (subGraph.size() <= 1) {
        sLogger.fatal(
            "Subgraph should have more than 1 node. Subgraph:" +
                StringUtils.join(subGraph, ","),
            new RuntimeException("Invalid Output"));
      }
      outPair.value().clear();
      outPair.value().addAll(subGraph);

      try {
        writer.append(outPair);
      } catch (IOException e) {
        sLogger.fatal("Couldn't write component:", e);
      }
    }
    try {
      writer.close();
    } catch (IOException e) {
      sLogger.fatal("Couldn't close:" + getOutPath().toString(), e);
      System.exit(-1);
    }

    sLogger.info(
        String.format(
            "Number of input groups: %d", numGroups));
    sLogger.info(String.format(
        "Outputted %d nodes in %d subgraphs", numNodesInSubGraphs,
        graphId + 1));
    sLogger.info(String.format(
        "Number of merges that would have exceeded max group size: %d",
        numMergeTooLarge));
  }

  public static void main(String[] args) throws Exception {
    SelectThreadableGroups stage = new SelectThreadableGroups();
    int res = stage.run(args);
    System.exit(res);
  }
}

