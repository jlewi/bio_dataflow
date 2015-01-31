package contrail.correct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.sequences.FastQRecord;
import contrail.stages.AvroCollectorMock;

public class TestJoinReads {
  @Test
  public void testmapper() {
    FastQRecord inRecord = new FastQRecord();
    inRecord.setId("SRR20/1");
    inRecord.setQvalue("!!!!!");
    inRecord.setRead("ACTGGT");

    JoinReads.JoinMapper mapper = new JoinReads.JoinMapper();
    JobConf conf = new JobConf();
    mapper.configure(conf);

    AvroCollectorMock<Pair<CharSequence, FastQRecord>> collector =
        new AvroCollectorMock<Pair<CharSequence, FastQRecord>>();
    ReporterMock reporter = new ReporterMock();
    try {
      mapper.map(inRecord, collector, reporter);
    } catch (IOException e) {
      fail(e.getStackTrace().toString());
    }
    assertEquals("SRR20", collector.data.get(0).key().toString());
  }
}
