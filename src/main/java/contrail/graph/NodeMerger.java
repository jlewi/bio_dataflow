/**
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
package contrail.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import contrail.ContrailConfig;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;

/**
 * A class for merging chains of nodes.
 *
 */
public class NodeMerger {
  private static final Logger sLogger = Logger.getLogger(NodeMerger.class);
  ArrayList<R5Tag> allTags;

  public NodeMerger() {
    allTags = new ArrayList<R5Tag>();
  }

  /**
   * Copy edges from old_node to new node.
   * @param new_node: The node to copy to.
   * @param new_strand: Which strand of the node node we are considering.
   * @param old_node: The node to copy from.
   * @param old_strand: Which strand of the old node we are considering.
   * @param direction: Whether to consider  incoming or outgoing edges.
   * @param exclude: List of node ids to exclude from the copy.
   */
  protected void copyEdgesForStrand(
      GraphNode new_node, DNAStrand new_strand, GraphNode old_node,
      DNAStrand old_strand, EdgeDirection direction, HashSet<String> exclude) {

    List<EdgeTerminal> terminals =
      old_node.getEdgeTerminals(old_strand, direction);

    for (Iterator<EdgeTerminal> it = terminals.iterator();
         it.hasNext();) {
      EdgeTerminal terminal = it.next();
      if (exclude.contains(terminal.nodeId)) {
        continue;
      }

      // TODO(jlewi): We need to get the tags.
      List<CharSequence> tags = old_node.getTagsForEdge(old_strand, terminal);

      if (direction == EdgeDirection.INCOMING) {
        new_node.addIncomingEdgeWithTags(
            new_strand, terminal, tags, ContrailConfig.MAXTHREADREADS);
      } else {
        new_node.addOutgoingEdgeWithTags(
            new_strand, terminal, tags, ContrailConfig.MAXTHREADREADS);
      }
    }
  }

  /**
   * Make a copy of the R5Tags.
   * @param tags
   * @return
   */
  protected List<R5Tag> copyR5Tags(List<R5Tag> tags) {
    List<R5Tag> newList = new ArrayList<R5Tag>();
    for (R5Tag tag: tags) {
      R5Tag copy = new R5Tag();
      copy.setTag(tag.getTag());
      copy.setStrand(tag.getStrand());
      copy.setOffset(tag.getOffset());
      newList.add(copy);
    }
    return newList;
  }

  /**
   * Reverse a list of R5 tags in place.
   * @param tags
   * @return
   */
  protected void reverseReads(List<R5Tag> tags, int length) {
    for (R5Tag tag: tags) {
      R5TagUtil.reverse(tag, length);
    }
  }

  /**
   * Check the two nodes overlap.
   * @param src
   * @param dest
   * @param nodes
   * @param overlap
   * @return
   */
  private boolean checkOverlap(
      Sequence src, Sequence dest, int overlap) {
    Sequence srcSuffix = src.subSequence(src.size() - overlap, src.size());
    Sequence destPrefix = dest.subSequence(0, overlap);
    return srcSuffix.equals(destPrefix);
  }

  /**
   * Merge the sequences.
   *
   */
  protected Sequence mergeSequences(
      List<EdgeTerminal> chain, Map<String, GraphNode> nodes,
      int overlap) {
    allTags.clear();
    // Figure out how long the merged sequence is.
    int length = 0;
    for (EdgeTerminal terminal : chain) {
      // Don't count the overlap because it will be counted in the next
      // sequence.
      length += nodes.get(terminal.nodeId).getSequence().size() - overlap;
    }
    length += overlap;

    Sequence mergedSequence = new Sequence(DNAAlphabetFactory.create(), length);

    boolean isFirst = true;

    // Keep track of the sequences to be merged.
    Sequence lastSequence = null;
    Sequence currentSequence = null;

    // Keep track of the forward and reverse offsets.
    int forwardOffset = 0;
    for (EdgeTerminal terminal : chain) {
      currentSequence = nodes.get(terminal.nodeId).getSequence();
      currentSequence = DNAUtil.sequenceToDir(currentSequence, terminal.strand);

      Sequence nonOverlap = currentSequence;
      if (!isFirst) {
        // Check we overlap with the previous sequence.
        if (!checkOverlap(lastSequence, currentSequence, overlap)) {
          throw new RuntimeException(
              "Can't merge nodes. Sequences don't overlap by: " + overlap + " " +
              "bases.");
        }

        // For all but the first node we truncate the first overlap bases
        // because we only want to add them once.
        nonOverlap = nonOverlap.subSequence(overlap, currentSequence.size());
      } else {
        isFirst = false;
      }
      mergedSequence.add(nonOverlap);
      // We can't simply use nonOverlap because nonOverlap might be less than
      // K-1;
      lastSequence = currentSequence;

      // Align the R5Tags.
      List<R5Tag> tags = nodes.get(terminal.nodeId).getData().getR5Tags();
      tags = copyR5Tags(tags);
      if (terminal.strand == DNAStrand.REVERSE) {
        // Reverse the tags.
        reverseReads(tags, nodes.get(terminal.nodeId).getSequence().size());
      }
      // Now we need to shift the offset to account for the joined sequences.
      // The offset is also the position of the base in the sequence that
      // corresponds to the first base in the read. The offset is independent
      // of which strand is aligned to the read.
      for (R5Tag tag : tags) {
         tag.setOffset(tag.getOffset() + forwardOffset);
      }
      allTags.addAll(tags);

      // Increment the offsets.
      forwardOffset += currentSequence.size() - overlap;
    }

    return mergedSequence;
  }

  /**
   * Compute the coverage for the result of merging two nodes.
   *
   * The resulting coverage is a weighted average of the source and destination
   * coverages. The weights are typically the length measured in # of Kmers
   * spanning the sequence; as opposed to base pairs. Consequently, the length
   * of a sequence is typically len(sequence) - K + 1, where len(sequence)
   * is the number of base pairs.
   * @param src_coverage
   * @param src_coverage_length
   * @param dest_coverage
   * @param dest_coverage_length
   * @return
   */
  protected float computeCoverage(
      Collection<EdgeTerminal> chain, Map<String, GraphNode> nodes,
      int overlap) {
    float coverageSum = 0;
    float weightSum = 0;

    Iterator<EdgeTerminal> itTerminal = chain.iterator();
    while (itTerminal.hasNext()) {
      EdgeTerminal terminal = itTerminal.next();
      GraphNode node = nodes.get(terminal.nodeId);
      float weight = node.getSequence().size() - overlap;
      coverageSum += node.getCoverage() * weight;
      weightSum += weight;
    }
    return coverageSum/weightSum;
  }

  /**
   * Container for the result of merging two nodes.
   */
  public static class MergeResult {
    // The merged node.
    public GraphNode node;

    // Which strand the merged sequence corresponds to.
    public DNAStrand strand;
  }

  /**
   * Merge a bunch of nodes into a new node.
   * @param newId: The id to assign the merged node.
   * @param chain: A collection of the terminals to merge.
   * @param nodes: A map containing the actual nodes. Should also
   *   contain nodes with edges to the nodes at the ends of
   *   terminals if moving the edges.
   * @param overlap: The number of bases that should overlap between
   *   the two sequences.
   * @return
   * @throws RuntimeException if the nodes can't be merged.
   *
   * The coverage lengths for the source and destination are automatically
   * computed as the number of KMers overlapping by K-1 bases would span
   * the source and destination sequences. K = overlap +1.
   */
  public MergeResult mergeNodes(
      String newId, List<EdgeTerminal> chain, Map<String, GraphNode> nodes,
      int overlap) {
    Sequence mergedSequence = mergeSequences(chain, nodes, overlap);
    float coverage = computeCoverage(chain, nodes, overlap);

    Sequence canonicalSequence = DNAUtil.canonicalseq(mergedSequence);
    DNAStrand mergedStrand = DNAUtil.canonicaldir(mergedSequence);

    GraphNode newNode = new GraphNode();
    newNode.setNodeId(newId);
    newNode.setCoverage(coverage);
    newNode.setSequence(canonicalSequence);

    // Reverse the reads if necessary.
    if (mergedStrand == DNAStrand.REVERSE) {
      reverseReads(allTags, mergedSequence.size());
    }
    // We don't want to steal a reference to allTags because allTags
    // gets reused.
    newNode.getData().getR5Tags().addAll(allTags);
    allTags.clear();

    // List of nodes in the chain.
    HashSet<String> idsInChain = new HashSet<String>();
    for (EdgeTerminal terminal : chain) {
      idsInChain.add(terminal.nodeId);
    }

    EdgeTerminal startTerminal = chain.get(0);
    EdgeTerminal endTerminal = chain.get(chain.size() - 1);
    GraphNode startNode = nodes.get(chain.get(0).nodeId);
    GraphNode endNode = nodes.get(chain.get(chain.size() - 1).nodeId);
    // Preserve the edges we need to preserve the incoming/outgoing
    // edges corresponding to the strands but we also need to consider
    // the reverse complement of the strand.
    // Add the incoming edges. When copying edges to the chain
    // we exclude any edges to nodes in the chain. These edges arise
    // in the case of cycles which are handled separatly.
    copyEdgesForStrand(
        newNode, mergedStrand, startNode, startTerminal.strand,
        EdgeDirection.INCOMING, idsInChain);

    // add the outgoing edges.
    copyEdgesForStrand(
        newNode, mergedStrand, endNode, endTerminal.strand,
        EdgeDirection.OUTGOING, idsInChain);

    // Now add the incoming and outgoing edges for the reverse complement.
    DNAStrand rcStrand = DNAStrandUtil.flip(mergedStrand);

    copyEdgesForStrand(
        newNode, rcStrand, endNode, DNAStrandUtil.flip(endTerminal.strand),
        EdgeDirection.INCOMING, idsInChain);

    // add the outgoing edges.
    copyEdgesForStrand(
        newNode, rcStrand, startNode, DNAStrandUtil.flip(startTerminal.strand),
        EdgeDirection.OUTGOING, idsInChain);

    // TODO(jeremy@lewi.us): We should add options which allow cycles to be
    // broken.
    // Handle a cycle. Suppose we have the graph A->B->A.
    // The merged graph should be AB->AB which implies
    //  RC(AB)->RC(AB).
    if (endNode.getEdgeTerminalsSet(
            endTerminal.strand, EdgeDirection.OUTGOING).contains(
                startTerminal)) {


      // TODO(jlewi): We need to get the tags.
      List<CharSequence> tags = endNode.getTagsForEdge(
          endTerminal.strand, startTerminal);

      EdgeTerminal incomingTerminal = new EdgeTerminal(newId, mergedStrand);
      newNode.addIncomingEdgeWithTags(
            mergedStrand, incomingTerminal, tags,
            ContrailConfig.MAXTHREADREADS);

      // Now add the outgoing edge to itself
      EdgeTerminal outgoingTerminal = new EdgeTerminal(
          newId, mergedStrand);
      newNode.addOutgoingEdgeWithTags(
          mergedStrand, outgoingTerminal, tags,
          ContrailConfig.MAXTHREADREADS);
    }

    MergeResult result = new MergeResult();
    result.node = newNode;
    result.strand = mergedStrand;
    return result;
  }
}
