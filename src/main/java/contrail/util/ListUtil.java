package contrail.util;

import java.util.Hashtable;
import java.util.List;

public class ListUtil {

  /**
   * Construct a hash table where the key is an element in the list
   * and the value is the count of how many times the element appears.
   */
  public static <E> Hashtable<E, Integer> countElements (List<E> list) {
    Hashtable<E, Integer> table = new Hashtable<E, Integer>();
    for (E element: list) {
      if (!table.containsKey(element)) {
        table.put(element, new Integer(0));
      }
      Integer count = table.get(element) + 1;
      table.put(element, count);
    }
    return table;
  }

  /**
   * Return true if the two hashtables are equal.
   */
  public static <E> boolean countsAreEqual (
      Hashtable<E, Integer> a, Hashtable<E, Integer> b) {
    if (a.size() != b.size()) {
      return false;
    }

    // Testing equality of the entry sets doesn't appear to work.
    for (E key: a.keySet()) {
      if (!a.get(key).equals(b.get(key))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if the contents of two lists are equal.
   *
   * This function assumes the element type implements the hash code
   * function and hashing is consistent with equals.
   * @param a
   * @param b
   * @return
   */
  public static <E> boolean listsAreEqual(List<E> a, List<E> b ) {

    if (a.size() != b.size()) {
      return false;
    }

    Hashtable<E, Integer> hash_a = countElements(a);
    Hashtable<E, Integer> hash_b = countElements(b);

    return countsAreEqual(hash_a, hash_b);
  }
}
