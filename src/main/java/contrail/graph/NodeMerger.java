package contrail.graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import contrail.ContrailConfig;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.sequences.StrandsForEdge;
import contrail.sequences.StrandsUtil;

public class NodeMerger {


  /**
   * Copy edges from old_node to new node.
   * @param new_node: The node to copy to.
   * @param new_strand: Which strand of the node node we are considering.
   * @param old_node: The node to copy from.
   * @param old_strand: Which strand of the old node we are considering.
   * @param direction: Whether to consider  incoming or outgoing edges.
   */
  protected static void copyEdgesForStrand(
      GraphNode new_node, DNAStrand new_strand, GraphNode old_node,
      DNAStrand old_strand,
      EdgeDirection direction) {

    List<EdgeTerminal> terminals =
      old_node.getEdgeTerminals(old_strand, direction);


    for (Iterator<EdgeTerminal> it = terminals.iterator();
         it.hasNext();) {
      EdgeTerminal terminal = it.next();

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
  protected static List<R5Tag> copyR5Tags(List<R5Tag> tags) {
    List<R5Tag> new_list = new ArrayList<R5Tag>();
    for (R5Tag tag: tags) {
      R5Tag copy = new R5Tag();
      copy.setTag(tag.getTag());
      copy.setStrand(tag.getStrand());
      copy.setOffset(tag.getOffset());
      new_list.add(copy);
    }
    return new_list;
  }

  /**
   * Reverse a list of R5 tags in place.
   * @param tags
   * @return
   */
  protected static void reverseReads(List<R5Tag> tags, int length) {
    for (R5Tag tag: tags) {
      R5TagUtil.reverse(tag, length);
    }
  }

  /**
   * Datastructure for returning information about the merging of
   * two sequences.
   */
  protected static class MergeInfo {
    // The canonical representation of the merged sequences.
    public Sequence canonical_merged;

    // merged_strand is the strand the merged sequence came from.
    public DNAStrand merged_strand;

    public int src_size;
    public int dest_size;

    // Whether we need to reverse the read tags.
    public boolean src_reverse;
    public int src_shift;
    public boolean dest_reverse;
    public int dest_shift;
  }

  /**
   * Warning: This function potentially modifies the inputs so
   * make copies.
   */
  protected static MergeInfo mergeSequences(
      Sequence canonical_src, Sequence canonical_dest,
      StrandsForEdge strands, int overlap) {
    MergeInfo info = new MergeInfo();

    // Save the original sizes.
    info.src_size = canonical_src.size();
    info.dest_size = canonical_dest.size();

    Sequence src_sequence = DNAUtil.sequenceToDir(
        canonical_src, StrandsUtil.src(strands));

    Sequence dest_sequence = DNAUtil.sequenceToDir(
        canonical_dest, StrandsUtil.dest(strands));

    // Check the overlap.
    // TODO(jlewi): We could probably make this comparison more efficient.
    // Copying the data to form new sequences is probably inefficient.
    Sequence src_overlap = src_sequence.subSequence(
        src_sequence.size() - overlap, src_sequence.size());
    Sequence dest_overlap = dest_sequence.subSequence(0, overlap);

    if (!src_overlap.equals(dest_overlap)) {
      throw new RuntimeException(
          "Can't merge nodes. Sequences don't overlap by: " + overlap + " " +
          "bases.");
    }

    // Combine the sequences.
    {
      Sequence merged = src_sequence;
      // Set src_sequence to null because it is no longer valid,
      // because we will modify it.
      src_sequence = null;
      Sequence dest_nonoverlap = dest_sequence.subSequence(
          overlap, dest_sequence.size());
      merged.add(dest_nonoverlap);

      info.merged_strand = DNAUtil.canonicaldir(merged);
      info.canonical_merged = DNAUtil.canonicalseq(merged);
    }

    // Determine whether we need to reverse the reads.
    // We need to reverse the reads if the direction of the merged
    // sequence and the original src_sequence don't match
    // Let A = The strand from the source node.
    // Let B = The strand from the destination node.
    if (info.merged_strand == DNAStrand.FORWARD) {
      // So AB < RC(AB)
      // The fragment from the source node appears first.
      if (StrandsUtil.src(strands) == DNAStrand.FORWARD) {
        // A < RC(A)
        // The src strand is already aligned.
        info.src_reverse = false;
      } else {
        // A > RC(A)
        // We have to reverse the source strand but we don't need to flip it.
        info.src_reverse = true;
      }
      info.src_shift = 0;

      if (StrandsUtil.dest(strands) == DNAStrand.FORWARD) {
        // We don't need to reverse this strand.
        info.dest_reverse = false;
      } else {
        info.dest_reverse = true;
      }
      // We need to shift it by the length of the unique part of the src.
      info.dest_shift = info.src_size - overlap;

    } else {
      // AB > RC(AB).
      // So the canonical representation is: RC(B)RC(A)
      // The strand from the destination node appears at the start
      // of the canonical representation of the merged sequence.
      if (StrandsUtil.src(strands) == DNAStrand.FORWARD) {
        // The reverse complement of the src strand appears
        // at the end of the merged sequence.
        info.src_reverse = true;
      } else {
        info.src_reverse = false;
      }
      info.src_shift = info.dest_size - overlap;

      if (StrandsUtil.dest(strands) == DNAStrand.FORWARD) {
        // We don't need to reverse this strand.
        info.dest_reverse = true;
      } else {
        info.dest_reverse = false;
      }
      // We don't shift it because the destination sequence appears first.
      info.dest_shift = 0;
    }
    return info;
  }

  /**
   * Construct a list of the R5 tags aligned with the merged sequence.
   * @param info
   * @param src_r5tags
   * @param dest_r5tags
   * @return
   */
  protected static List<R5Tag> alignTags(
      MergeInfo info, List<R5Tag> src_r5tags, List<R5Tag> dest_r5tags) {
    // Update the read alignment tags (R5Fields).
    // Make a copy of src tags.
    List<R5Tag> src_tags = copyR5Tags(src_r5tags);
    List<R5Tag> dest_tags = copyR5Tags(dest_r5tags);

    // Reverse the reads if necessary.
    if (info.src_reverse) {
      reverseReads(src_tags, info.src_size);
    }

    if (info.dest_reverse) {
      reverseReads(dest_tags, info.dest_size);
    }

    if (info.src_shift > 0) {
      for (R5Tag tag : src_tags) {
        tag.setOffset(tag.getOffset() + info.src_shift);
      }
    }

    if (info.dest_shift > 0) {
      for (R5Tag tag : dest_tags) {
        tag.setOffset(tag.getOffset() + info.dest_shift);
      }
    }

    src_tags.addAll(dest_tags);
    return src_tags;
  }

  /**
   * Compute the coverage for the result of merging two nodes.
   *
   * The resulting coverage is a weighted average of the source and destination
   * coverages. The weights are typically the length measured in # of Kmers
   * spanning the sequence; as opposed to base pairs. Consequently, the length
   * of a sequence is typically len(sequence) - K + 1, when len(sequence)
   * is the nubmer of pairs pairs.
   * @param src_coverage
   * @param src_coverage_length
   * @param dest_coverage
   * @param dest_coverage_length
   * @return
   */
  protected static float computeCoverage(
      float src_coverage, int src_coverage_length, float dest_coverage,
      int dest_coverage_length) {

    float coverage = (src_coverage * src_coverage_length) +
        (dest_coverage * dest_coverage_length);
    coverage = coverage / (src_coverage_length + dest_coverage_length);
    return coverage;
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
   * Merge two nodes
   * @param src: The source node
   * @param dest: The destination node
   * @param strands: Which strands the edge from src->dest comes from.
   * @param overlap: The number of bases that should overlap between
   *   the two sequences.
   * @param src_coverage_length: The length to associate with the source
   *   for the purpose of computing the coverage. This is typically the number
   *   of KMers in the source i.e it is len(src) - K + 1.
   * @param dest_coverage_length: The length to associate with the destination
   *   for the purpose of computing the coverage.
   * @return
   * @throws RuntimeException if the nodes can't be merged.
   */
  public static MergeResult mergeNodes(
      GraphNode src, GraphNode dest, StrandsForEdge strands, int overlap,
      int src_coverage_length, int dest_coverage_length) {
    // To merge two nodes we need to
    // 1. Form the combined sequences
    // 2. Update the coverage
    // 3. Remove outgoing edges from src.
    // 4. Remove Incoming edges to dest
    // 5. Add Incoming edges to src
    // 6. Add outgoing edges from dest
    Sequence src_sequence = src.getSequence();
    Sequence dest_sequence = dest.getSequence();
    MergeInfo merge_info = mergeSequences(
        src_sequence, dest_sequence, strands, overlap);

    GraphNode new_node = new GraphNode();
    new_node.setSequence(merge_info.canonical_merged);

    // Preserve the edges we need to preserve the incoming/outgoing
    // edges corresponding to the strands but we also need to consider
    // the reverse complement of the strand.
    // Add the incoming edges.
    copyEdgesForStrand(
        new_node, merge_info.merged_strand, src, StrandsUtil.src(strands),
        EdgeDirection.INCOMING);

    // add the outgoing edges.
    copyEdgesForStrand(
        new_node, merge_info.merged_strand, dest, StrandsUtil.dest(strands),
        EdgeDirection.OUTGOING);

    // Now add the incoming and outgoing edges for the reverse complement.
    DNAStrand rc_strand = DNAStrandUtil.flip(merge_info.merged_strand);
    StrandsForEdge rc_edge_strands = StrandsUtil.complement(strands);
    copyEdgesForStrand(
        new_node, rc_strand, dest, StrandsUtil.src(rc_edge_strands),
        EdgeDirection.INCOMING);

    // add the outgoing edges.
    copyEdgesForStrand(
        new_node, rc_strand, src, StrandsUtil.dest(rc_edge_strands),
        EdgeDirection.OUTGOING);

    // Align the tags.
    new_node.getData().setR5Tags(alignTags(
            merge_info, src.getData().getR5Tags(), dest.getData().getR5Tags()));

    // Compute the coverage.
    new_node.getData().setCoverage(computeCoverage(
        src.getCoverage(), src_coverage_length, dest.getCoverage(),
        dest_coverage_length));

    MergeResult result = new MergeResult();
    result.node = new_node;
    result.strand = merge_info.merged_strand;
    return result;
  }

  /**
   * Merge two nodes with a coverage length based on the overlap.
   * @param src: The source node
   * @param dest: The destination node
   * @param strands: Which strands the edge from src->dest comes from.
   * @param overlap: The number of bases that should overlap between
   *   the two sequences.
   * @return
   * @throws RuntimeException if the nodes can't be merged.
   *
   * The coverage lengths for the source and destination are automatically
   * computed as the number of KMers overlapping by K-1 bases would span
   * the source and destination sequences. K = overlap +1.
   */
  public static MergeResult mergeNodes(
      GraphNode src, GraphNode dest, StrandsForEdge strands, int overlap) {
    // Coverage length is how many KMers overlapping by K-1 bases
    // span the sequence where K = overlap + 1
    int K = overlap + 1;
    int src_coverage_length =
        src.getSequence().size() - K + 1;
    int dest_coverage_length =
        dest.getSequence().size() - K + 1;

      return mergeNodes(
          src, dest, strands, overlap, src_coverage_length,
          dest_coverage_length);
  }
}
