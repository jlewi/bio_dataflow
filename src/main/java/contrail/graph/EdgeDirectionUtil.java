package contrail.graph;

import java.util.Random;

/**
 * Some Utilities for working with the EdgeDirection enum.
 */
public class EdgeDirectionUtil {
	/**
	 * @return: The opposite or flipped direction.
	 */
	public static EdgeDirection flip(EdgeDirection direction) {
		if (direction == EdgeDirection.INCOMING) {
			return EdgeDirection.OUTGOING;
		} else {
			return EdgeDirection.INCOMING;
		}
	}

	/**
	 * Return a random direction.
	 */
	public static EdgeDirection random() {
		return Math.random() < .5 ?
		    EdgeDirection.INCOMING : EdgeDirection.OUTGOING;
	}

	/**
	 * Return a random direction using the supplied generator
	 */
	public static EdgeDirection random(Random generator) {
		return generator.nextDouble() < .5 ?
		    EdgeDirection.INCOMING : EdgeDirection.OUTGOING;
	}
}
