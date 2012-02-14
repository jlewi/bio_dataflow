package contrail.graph;

import java.util.Random;

import contrail.sequences.DNAStrand;

/**
 * Used to indicate direction of an edge relative to a node.
 *
 */
public enum EdgeDirection {	
	OUTGOING, // An outgoing edge
	INCOMING;  // An incoming edge;
	
	/**
	 * @return: The opposite or flipped direction.
	 */
	public EdgeDirection flip() {
		if (this == INCOMING) {
			return OUTGOING;
		} else {
			return INCOMING;
		}
	}
	
	/**
	 * Return a random direction.
	 */
	public static EdgeDirection random() {
		return Math.random() < .5 ? INCOMING : OUTGOING;
	}
	
	/**
	 * Return a random direction using the supplied generator
	 */
	public static EdgeDirection random(Random generator) {
		return generator.nextDouble() < .5 ? INCOMING : OUTGOING;
	}
	
}
