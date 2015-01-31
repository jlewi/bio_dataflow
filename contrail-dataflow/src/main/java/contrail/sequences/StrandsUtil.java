package contrail.sequences;

import java.util.Random;

/**
 * strands enumeration is used to indicate which strands of DNA (in canonical form)
 * the source and destination sequence in an edge come strands.FRom.
 *
 * @author jlewi
 *
 */
public class StrandsUtil {
  private static Random cls_generator;
	/**
	 * Function computes the corresponding edge that would
	 * come from the reverse complement of the strand which produced this edge.
	 * Do not confuse strands with flipping an edge to find the incoming
	 * edges to a node.
	 */
	public static StrandsForEdge complement(StrandsForEdge strands) {
	    if (strands.equals(StrandsForEdge.FF)) { return StrandsForEdge.RR; }
	    if (strands.equals(StrandsForEdge.FR)) { return StrandsForEdge.FR; }
	    if (strands.equals(StrandsForEdge.RF)) { return StrandsForEdge.RF; }
	    return StrandsForEdge.FF;
	}

	/**
	 * Return the strand for the src.
	 */
	public static DNAStrand src(StrandsForEdge strands) {
		if (strands.equals(StrandsForEdge.FF) ||
		    strands.equals(StrandsForEdge.FR)) {
			return DNAStrand.FORWARD;
		}
		return DNAStrand.REVERSE;
	}

	/**
	 * @return The strand for the destination
	 */
	public static DNAStrand dest(StrandsForEdge strands) {
		if (strands.equals(StrandsForEdge.FF) ||
		    strands.equals(StrandsForEdge.RF)) {
			return DNAStrand.FORWARD;
		}
		return DNAStrand.REVERSE;
	}

	/**
	 * For the strands strands.FRom edge strands.FRom the strand for the source and destination.
	 * @param src
	 * @param dest
	 * @return
	 */
	public static StrandsForEdge form(DNAStrand src, DNAStrand dest) {
		if (src == DNAStrand.FORWARD) {
			if (dest == DNAStrand.FORWARD) {
				return StrandsForEdge.FF;
			} else {
				return StrandsForEdge.FR;
			}
		} else {
			if (dest == DNAStrand.FORWARD) {
				return StrandsForEdge.RF;
			} else {
				return StrandsForEdge.RR;
			}
		}
	}

	/**
   * Return a random value.
   */
  public static StrandsForEdge random(Random generator) {
    int index = generator.nextInt(StrandsForEdge.values().length);
    return StrandsForEdge.values()[index];
  }

  /**
   * Return a random value.
   */
  public static StrandsForEdge random() {
    if (cls_generator == null) {
      cls_generator = new Random();
    }
    return StrandsUtil.random(cls_generator);
  }


	/**
	 * Convert a string representation.
	 * @param src
	 * @param dest
	 * @return
	 */
	public static StrandsForEdge parse(String strands) {
		return StrandsForEdge.valueOf(strands.toUpperCase());
	}
}
