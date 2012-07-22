package contrail.avro;

/**
 * A list of mapreduce counters used to communicate between jobs.
 *
 * We use several mapreduce counters to control processing; e.g. CompressChains
 * uses counters in CompressibleAvro to determine how many nodes can be
 * compressed and whether compression is done. This class defines the names
 * of different counters so we can be consistent across different jobs.
 */
public class GraphCounters {
  public static class CounterName {
    public CounterName (String group_name, String tag_name) {
      group = group_name;
      tag = tag_name;
    }
    public final String group;
    public final String tag;
  }

  public static CounterName compressible_nodes =
      new CounterName("Contrail", "compressible");

  // The number of nodes marked in PairMarkAvro to be merged.
  public static CounterName num_nodes_to_merge =
      new CounterName("Contrail", "nodes_to_merge");

  // The number of nodes which still need to be compressed after PairMerge
  // runs.
  public static CounterName pair_merge_compressible_nodes =
      new CounterName("Contrail", "nodes_left_to_compress");
  
  public static CounterName quick_mark_nodes_send_to_compressor =
      new CounterName("Contrail", "nodes_to_send_to_compressor");
}
