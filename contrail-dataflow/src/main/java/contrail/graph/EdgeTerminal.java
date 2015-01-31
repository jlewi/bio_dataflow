package contrail.graph;
import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;

/**
 * An immutable class for representing the terminal for an edge.
 *
 * A terminal is identified by the id of the node as well as a DNAStrand
 * which specifies which strand of the DNA in the node corresponds to the
 * terminal.
 * @author Jeremy Lewi (jeremy@lewi.us)
 */
public final class EdgeTerminal {
  public final String nodeId;
  public final DNAStrand strand;

  private Integer hash_code;

  public EdgeTerminal(String node, DNAStrand dna_strand) {
	  nodeId = node;
	  strand = dna_strand;
  }

  public boolean equals(Object o) {
	  if (!(o instanceof EdgeTerminal)){
		  throw new RuntimeException(
				  "Can only compare to other EdgeTerminals.");
	  }
	  EdgeTerminal other = (EdgeTerminal) o;
	  if (strand != other.strand) {
		  return false;
	  }
	  return this.nodeId.equals(other.nodeId);
  }

  @Override
  public int hashCode() {
    if (hash_code == null) {
      String value = nodeId + ":" + DNAStrandUtil.toString(strand);
      hash_code = new Integer(value.hashCode());
    }
    return hash_code.intValue();
  }

  /**
   * A convenience method for displaying the value as a string.
   */
  public String toString() {
	  return nodeId + ":" + strand.toString();
  }

  /**
   * Return an EdgeTerminal identifying the opposite strand for this node.
   */
  public EdgeTerminal flip() {
    return new EdgeTerminal(nodeId, DNAStrandUtil.flip(strand));
  }
}
