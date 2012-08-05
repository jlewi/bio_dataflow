package contrail.avro;

import static org.junit.Assert.*;

import org.apache.avro.mapred.Pair;
import org.junit.Test;

public class TestAssertEquals {

  @Test
  public void test() {
    Pair<Integer, Integer> pair1 = new Pair<Integer, Integer>(1, 2);
    Pair<Integer, Integer> pair2 = new Pair<Integer, Integer>(1, 1);
    assertFalse(pair1.equals(pair2));
  }
}
