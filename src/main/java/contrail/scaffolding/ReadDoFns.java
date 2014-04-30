package contrail.scaffolding;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import contrail.sequences.FastQRecord;

/**
 * Do functions for processing reads.
 */
public class ReadDoFns {
  /**
   * Rekey the reads by the id.
   */
  public static class KeyFastQByIdDo
      extends DoFn<FastQRecord, Pair<String, FastQRecord>> {
    // TODO(jeremy@lewi.us): This probably belongs in FastQDoFns.
    @Override
    public void process(FastQRecord read,
        Emitter<Pair<String, FastQRecord>> emitter) {
      emitter.emit(new Pair(read.getId().toString(), read));
    }
  }
}
