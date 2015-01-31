/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author:Jeremy Lewi (jeremy@lewi.us)
package contrail.stages;

import java.util.Random;

/**
 * The CoinFlipper class maps a string to a random value. The random
 * value is based on a seed and the string passed in. This class is
 * primarily used by PairMarkAvro to determine which pairs of nodes can
 * be merged.
 */
public class CoinFlipper {
  /**
   * Construct a flipper using the given seed.
   * @param seed
   */
  public CoinFlipper(long seed) {
    rfactory = new Random();
    randseed = seed;
  }

  /**
   * Enumeration defines the state for compressible nodes.
   * We don't use Heads and Tails because we already
   * use tails to refer to tails of a node. We don't want to use
   * Male/Female because the abbreviation "F" gets confused with the
   * forward strand of DNA. So we use Up and Down.
   */
  public enum CoinFlip {
    UP, DOWN;
  }

  /**
   * Flip a coin. The seed for the random generator is a combination of a
   * seed passed to the constructor and the argument to flip.
   * @param string_seed
   * @return
   */
  public CoinFlip flip(String string_seed) {
    rfactory.setSeed(string_seed.hashCode() ^ randseed);

    double rand = rfactory.nextDouble();
    CoinFlip side = (rand >= .5) ? CoinFlip.UP : CoinFlip.DOWN;
    return side;
  }

  private static long randseed = 0;
  private Random rfactory = new Random();
}
