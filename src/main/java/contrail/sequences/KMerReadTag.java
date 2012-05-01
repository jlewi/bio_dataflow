package contrail.sequences;

/**
 * This class represents a tag which is used to identify where a kmer
 * came from. To identify the read a kmer came from we use 1) the
 * id for the read it came from and 2) an integer which can be used to resolve
 * the location of the KMer within the read when that KMer appears multiple
 * times within the read.
 *
 * This class is a wrapper for the GraphNodeTagKMer schema.
 * TODO(jlewi): Either this class or the schema GraphNodeTagKMer should be renamed
 * so the names are more consistent.
 *
 * @author jlewi
 *
 */
public class KMerReadTag implements Comparable {

  public final String read_id;
  public final int chunk;

  public KMerReadTag(String read_id, int chunk) {
    this.read_id = read_id;
    this.chunk = chunk;
  }

  /**
   * Compare to sequences.
   *
   * TODO(jlewi): Add a unittest.
   * TODO(jlewi): How should we handle sequences with different alphabets.
   * @param seq
   * @return
   */
  public int compareTo(Object other) {
    if (!(other instanceof KMerReadTag)) {
      throw new RuntimeException("Can only compare sequences.");
    }

    KMerReadTag other_tag = (KMerReadTag) other;
    // Compare the read id's
    int read_compare = this.read_id.compareTo(other_tag.read_id);

    if (read_compare != 0) {
      return read_compare;
    }

    // Compare the chunks.
    if (this.chunk < other_tag.chunk) {
      return -1;
    }
    if (this.chunk == other_tag.chunk) {
      return 0;
    }

    return 1;
  }

  public String toString() {

    if (chunk > 0) {
      return read_id + "c" + chunk;
    }
    else {
      return read_id;
    }
  }
}
