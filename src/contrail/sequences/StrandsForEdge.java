package contrail.sequences;

/**
 * This enumeration is used to indicate which strand of DNA (in canonical form)
 * the source and destination sequence in an edge come from. 
 * 
 * @author jlewi
 *
 */
public enum StrandsForEdge {
	FF,
	FR,
	RF,
	RR;
	
	/**
	 * @return The flipped the edge.
	 */
	public StrandsForEdge flip() {
	    if (this.equals(FF)) { return RR; }
	    if (this.equals(FR)) { return FR; }
	    if (this.equals(RF)) { return RF; }
	    //if (this.equals(RR)) { return FF; }
	    return FF;
	}
	
	/**
	 * Return the strand for the src.
	 */
	public DNAStrand src() {
		if (this.equals(FF) || this.equals(FR)) {
			return DNAStrand.FORWARD;
		}
		return DNAStrand.REVERSE;
	}

	/**
	 * @return The strand for the destination
	 */
	public DNAStrand dest() {
		if (this.equals(FF) || this.equals(RF)) {
			return DNAStrand.FORWARD;
		}
		return DNAStrand.REVERSE;
	}
}
