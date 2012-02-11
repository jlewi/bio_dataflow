package contrail.graph;
import contrail.sequences.DNAStrand;

/**
 * An immutable class for representing the terminal for an edge. 
 * 
 * A terminal is indentified by the id of the node as well as a DNAStrand
 * which specifies which strand of the DNA in the node corresponds to the 
 * terminal.
 * @author Jeremy Lewi (jeremy@lewi.us)
 */
public final class EdgeTerminal {
  public final String nodeId;
  public final DNAStrand strand;
  public EdgeTerminal(String node, DNAStrand dna_strand) {
	  nodeId = node;
	  strand = dna_strand;
  }  
}
