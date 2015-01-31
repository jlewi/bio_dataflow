package contrail.sequences;

import java.util.Random;

/**
 * Enumeration which defines which strand a sequence comes from, the forward
 * or reverse strand. This usually refers to the canonical directions.
 */
public class DNAStrandUtil {
	/**
	 * @return Flip the direction
	 */
	public static DNAStrand flip(DNAStrand strand) {
		if (strand == DNAStrand.FORWARD) {
			return DNAStrand.REVERSE;
		} else {
			return DNAStrand.FORWARD;
		}
	}

	/**
	 * Return a random strand.
	 */
	public static DNAStrand random() {
		return Math.random() < .5 ? DNAStrand.FORWARD : DNAStrand.REVERSE;
	}

	/**
	 * Return a random strand using the supplied generator.
	 */
	public static DNAStrand random(Random generator) {
		return generator.nextDouble() < .5 ? DNAStrand.FORWARD : DNAStrand.REVERSE;
	}
	/**
	 * For backwards compatibility with string representation.
	 */
	public static String toString(DNAStrand strand) {
		if (strand == DNAStrand.FORWARD) {
			return "f";
		} else {
			return "r";
		}
	}
}
