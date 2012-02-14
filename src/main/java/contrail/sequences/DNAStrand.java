package contrail.sequences;

import java.util.Random;

/**
 * Enumeration which defines which strand a sequence comes from, the forward
 * or reverse strand. This usually refers to the canonical directions.
 */
public enum DNAStrand {
	FORWARD,
	REVERSE;
	/**
	 * @return Flip the direction
	 */
	public DNAStrand flip() {
		if (this == FORWARD) {
			return REVERSE;
		} else {
			return FORWARD;
		}
	}
	
	/**
	 * Return a random strand.
	 */
	public static DNAStrand random() {
		return Math.random() < .5 ? FORWARD : REVERSE;
	}
	
	/**
	 * Return a random strand using the supplied generator.
	 */
	public static DNAStrand random(Random generator) {
		return generator.nextDouble() < .5 ? FORWARD : REVERSE;
	}
	/**
	 * For backwards compatibility with string representation.
	 */
	public String toString() {
		if (this == FORWARD) {
			return "f";
		} else {
			return "r";
		}
	}	
}
