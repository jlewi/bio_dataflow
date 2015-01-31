package contrail.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.UnsupportedEncodingException;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import contrail.sequences.DNAStrand;
import contrail.sequences.DNAStrandUtil;
import contrail.util.ByteUtil;

public class TestEdgeTerminal {

  // Random number generator.
  private Random generator;

  @Before
  public void setUp() {
    generator = new Random();
  }

  public String randomString(int length) {
    // Generate random byte values and then convert them to a
    // string using ascii.
    byte[] bytes = new byte[length];

    for (int pos = 0; pos < length; pos++) {
      int rnd_int = generator.nextInt(255);
      bytes[pos] = ByteUtil.uintToByte(rnd_int);
    }
    String random_string;
    try {
      random_string = new String(bytes, "ISO-8859-1");
    } catch (UnsupportedEncodingException exception) {
      throw new RuntimeException("Could not encode the random string.");
    }
    return random_string;
  }

  @Test
  public void testHashCode() {
    int ntrials = 10;
    for (int trial = 0; trial < ntrials; trial ++) {
      // Check that two terminals with the same terminal and
      // node id return the same hashcode.
      String terminalid = randomString(generator.nextInt(10) + 2);
      DNAStrand strand = DNAStrandUtil.random(generator);

      {
        EdgeTerminal terminal1 = new EdgeTerminal(terminalid, strand);
        EdgeTerminal terminal2 = new EdgeTerminal(terminalid, strand);
        assertEquals(terminal1.hashCode(), terminal2.hashCode());
      }

      {
        // Change the strand.
        EdgeTerminal terminal1 = new EdgeTerminal(terminalid, strand);
        EdgeTerminal terminal2 =
            new EdgeTerminal(terminalid, DNAStrandUtil.flip(strand));
        assertFalse(terminal1.hashCode() == terminal2.hashCode());
      }

      {
        // Change node id.
        String differentid = randomString(generator.nextInt(10) + 2);
        while (terminalid.equals(differentid)) {
          differentid = randomString(generator.nextInt(10) + 2);
        }
        EdgeTerminal terminal1 = new EdgeTerminal(terminalid, strand);
        EdgeTerminal terminal2 = new EdgeTerminal(differentid, strand);
        assertFalse(terminal1.hashCode() == terminal2.hashCode());
      }
    }
  }
}
