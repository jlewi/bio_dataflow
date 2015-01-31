package contrail.util;

/**
 * A tuple of two items.
 * @param <T1>
 * @param <T2>
 */
public class Tuple<T1, T2> {
  public final T1 first;
  public final T2 second;

  public Tuple(T1 t1, T2 t2) {
    first = t1;
    second = t2;
  }
}
