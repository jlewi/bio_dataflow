package contrail.sequences;

/**
 * This enumeration is used to indicate which strands of DNA (in canonical form)
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
	 * This function computes the corresponding edge that would
	 * come from the reverse complement of this edge. 
	 * Do not confuse this with flipping an edge to find the incoming
	 * edges to a node.
	 */
	public StrandsForEdge complement() {
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
	
	/** 
	 * For the strands from edge from the strand for the source and destination.
	 * @param src
	 * @param dest
	 * @return
	 */
	public static StrandsForEdge form(DNAStrand src, DNAStrand dest) {
		if (src == DNAStrand.FORWARD) {
			if (dest == DNAStrand.FORWARD) {
				return FF;
			} else {
				return FR;
			}
		} else {
			if (dest == DNAStrand.FORWARD) {
				return RF;
			} else {
				return RR;
			}
		}
	}
	
	/** 
	 * Convert a string representation.
	 * @param src
	 * @param dest
	 * @return
	 */
	public static StrandsForEdge parse(String link_dir) {
		link_dir = link_dir.toLowerCase();
		if (link_dir.equals("ff")) {
			return FF;
		} else if (link_dir.equals("fr")) {
			return FR;
		} else if (link_dir.equals("rf")) {
			return RF;
		} else if (link_dir.equals("rr")) {
			return RR;
		}
		throw new RuntimeException(
				"Not a valid value for StrandsForEdge:" + link_dir);
	}
}
