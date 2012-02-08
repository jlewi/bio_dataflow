package contrail.sequences;

/**
 * Enumeration which defines the different canonical directions for DNA
 * sequences.
 */
public enum DNADirection {
	FORWARD,
	REVERSE;
	/**
	 * @return Flip the direction
	 */
	public DNADirection flip() {
		if (this == FORWARD) {
			return REVERSE;
		} else {
			return FORWARD;
		}
	}
	
	/**
	 * Return a random direction
	 */
	public static DNADirection random() {
		return Math.random() < .5 ? FORWARD : REVERSE;
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
