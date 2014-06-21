package contrail.scaffolding;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;

import contrail.sequences.FastQRecord;
import contrail.sequences.Read;

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

  /**
   * Rekey the read by the id.
   *
   * Read is filtered out if no id is available.
   */
  public static class KeyByIdDo
      extends DoFn<Read, Pair<String, Read>> {
    // TODO(jeremy@lewi.us): This probably belongs in FastQDoFns.
    @Override
    public void process(Read read, Emitter<Pair<String, Read>> emitter) {
      String readId = "";
      if (read.getFastq() != null) {
        readId = read.getFastq().getId().toString();
      }

      if (readId.isEmpty()) {
        this.increment("contrail", "ReadDoFns-KeyByIdDo-read-no-id");
        return;
      }
      emitter.emit(new Pair(readId, read));
    }
  }
}
