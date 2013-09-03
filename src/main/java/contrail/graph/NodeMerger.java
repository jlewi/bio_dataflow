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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
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

    // Keep track of the forward offset.
    int forwardOffset = 0;
    for (EdgeTerminal terminal : chain) {
      GraphNode node = nodes.get(terminal.nodeId);
      // Align and truncate the sequence.
      Sequence nonOverlap = node.getSequence();
      nonOverlap = DNAUtil.sequenceToDir(nonOverlap, terminal.strand);

      if (mergedSequence.size() > 0) {
        // Check we overlap with the previous sequence.
        if (!GraphUtil.checkOverlap(mergedSequence, nonOverlap, overlap)) {
          throw new RuntimeException(
              "Can't merge nodes. Sequences don't overlap by: " + overlap + " " +
              "bases.");
        }

        // For all but the first node we truncate the first overlap bases
        // because these bases overlap with the suffix of mergedSequence.
        nonOverlap = nonOverlap.subSequence(overlap, nonOverlap.size());
      }
      mergedSequence.add(nonOverlap);

      // Align the R5Tags.
      List<R5Tag> tags = node.getData().getR5Tags();
      tags = copyR5Tags(tags);
      if (terminal.strand == DNAStrand.REVERSE) {
        // Reverse the tags.
        reverseReads(tags, node.getSequence().size());
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
      forwardOffset += node.getSequence().size() - overlap;
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

    // ->X->...->R(X) or
    // ->R(X)->...->X
    EdgeTerminal startTerminal = chain.get(0);
    EdgeTerminal endTerminal = chain.get(chain.size() - 1);
    boolean endIsRCStart =
        (startTerminal.nodeId.equals(endTerminal.nodeId)) &&
        (startTerminal.strand.equals(DNAStrandUtil.flip(endTerminal.strand)));
    boolean isPalindrome = DNAUtil.isPalindrome(canonicalSequence);
    if (isPalindrome) {
      // Assuming the graph is the result of the standard contrail stages
      // we can only get a palindrome if we have
      // ->X->...->R(X) or
      // ->R(X)->...->X
      // i.e suppose X->...->A = R(A)->...->R(X)
      // then X=R(A) so we have X->...->R(X)
      // Sanity check.
      if (!endIsRCStart) {
        // It is possible to construct a graph which doesn't satisfy this
        // condition. i.e suppose we have  CAT->ATCAT  the merged sequence
        // is CATCAT and the two nodes are not the same. However, this
        // graph should never be produced by the current set of contrail
        // operations. Currently, all nodes would start with the same K so
        // buildgraph would yield the graph CAT->ATC->TCA->CAT.
        // This check protects us in case other parts of the code change
        // and this assumption is violated.
        ArrayList<String> terminalStrings = new ArrayList<String>();
        for (EdgeTerminal t : chain) {
          terminalStrings.add(t.toString());
        }
        sLogger.fatal(
            "Something is wrong with the merge. The result of the merge " +
            "is a palindrome but the start and end terminal are not the same " +
            "nodes. The terminals to merge is: " +
            StringUtils.join(terminalStrings, ","),
            new RuntimeException("Unexpected palindrome."));
      }

      // We select the merged strand so that the strand of the incoming
      // and outgoing edges will be preserved.
      mergedStrand = startTerminal.strand;
    }
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

    // Copy the outgoing edges for the last node in the chain.
    // We don't want to copy an edge twice. Consider the special case
    // ->X->...->R(X).  So the incoming edges to X are the same edges
    // as the outgoing edges for R(X). So for the forward strand we only
    // need to copy the incoming edges. Similarly we don't need to consider
    // the reverse complement because that yields the same sequence.
    if (!endIsRCStart) {
      copyEdgesForStrand(
          newNode, mergedStrand, endNode, endTerminal.strand,
          EdgeDirection.OUTGOING, idsInChain);

      // Now add the incoming and outgoing edges for the reverse complement.
      DNAStrand rcStrand = DNAStrandUtil.flip(mergedStrand);

      copyEdgesForStrand(
          newNode, rcStrand, endNode, DNAStrandUtil.flip(endTerminal.strand),
          EdgeDirection.INCOMING, idsInChain);

      copyEdgesForStrand(
          newNode, rcStrand, startNode, DNAStrandUtil.flip(startTerminal.strand),
          EdgeDirection.OUTGOING, idsInChain);
    }

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

  /**
   * Merge a node that forms a chain with itself.
   *
   * If X->RC(X) the two strands of the node are connected and form a chain.
   * The forward and reverse strands of the node are the same. So any
   * edges to other nodes don't need to move because the sequence for both
   * strands of the merged sequence is the same.
   *
   * Note: The merge preserves the strand of any edges attached to the node.
   * e.g Suppose we have A->X->RC(X)->RC(A)
   * The merged graph is A->Y->RC(A)  where Y=RC(Y)
   * Node Y has a single outgoing edge to RC(A). This edge could be represented
   * as  FR or RR because Y is a palindrome.
   * However, Node A will store the edge A->Y as FF which is consistent with
   * Y->A being represented as RR. We want the two edges to remain consistent
   * because other parts of the code depend on the edges remaining consistent.
   *
   * @param node: The new node if the strands were merged or null if the
   * strands can't be merged.
   *
   * @return
   */
  public GraphNode mergeConnectedStrands(
      GraphNode node, int overlap) {
    if (!node.hasConnectedStrands()) {
      sLogger.fatal(
          "Tried to merge connected strands for a node without connected " +
          "strands.", new RuntimeException("Node's strands aren't connected."));
    }

    List<EdgeTerminal> terminals = new ArrayList<EdgeTerminal>();

    DNAStrand srcStrand = null;
    if (node.getEdgeTerminalsSet(
            DNAStrand.FORWARD, EdgeDirection.OUTGOING).contains(
                new EdgeTerminal(node.getNodeId(), DNAStrand.REVERSE))) {
      srcStrand = DNAStrand.FORWARD;
    } else {
      srcStrand = DNAStrand.REVERSE;
    }

    terminals.add(new EdgeTerminal(node.getNodeId(), srcStrand));
    terminals.add(new EdgeTerminal(
        node.getNodeId(), DNAStrandUtil.flip(srcStrand)));

    if (node.getEdgeTerminals(
            terminals.get(0).strand, EdgeDirection.OUTGOING).size() != 1) {
      // Strands can't be merged because src strand has more than 1 outgoing
      // edge.
      return null;
    }

    if (node.getEdgeTerminals(
        terminals.get(1).strand, EdgeDirection.INCOMING).size() != 1) {
      // Strands can't be merged because dest strand has more than 1 incoming
      // edge.
      return null;
    }

    HashMap<String, GraphNode> nodes = new HashMap<String, GraphNode>();
    nodes.put(node.getNodeId(), node);
    MergeResult result = mergeNodes(
        node.getNodeId(), terminals, nodes, overlap);

    return result.node;
  }
}
