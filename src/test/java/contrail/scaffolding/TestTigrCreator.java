package contrail.scaffolding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.ReporterMock;
import contrail.graph.GraphNode;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.stages.AvroCollectorMock;

public class TestTigrCreator {
  public class TestData {
    final public Object input;
    final public String key;
    final public Object value;

    TestData(Object input, String key, Object value) {
      this.input = input;
      this.key = key;
      this.value = value;
    }
  }

  @Test
  public void testMapperNode() {
    ArrayList<TestData> pairs = new ArrayList<TestData>();

    {
      GraphNode node = new GraphNode();
      node.setNodeId("contig");
      node.setSequence(new Sequence("ACTGG", DNAAlphabetFactory.create()));

      pairs.add(new TestData(
          node.getData(), node.getNodeId().toString(), node.clone().getData()));
    }

    {
      BowtieMapping mapping = new BowtieMapping();

      mapping = new BowtieMapping();
      mapping.setContigId("contig_2_0");
      mapping.setReadId("read_2_0_75");
      mapping.setContigStart(75);
      mapping.setContigEnd(94);
      mapping.setReadClearStart(0);
      mapping.setReadClearEnd(25);
      mapping.setNumMismatches(1);
      mapping.setRead(null);

      pairs.add(new TestData(
          mapping, mapping.getContigId().toString(), mapping));
    }

    JobConf job = new JobConf(TestTigrCreator.class);
    ReporterMock reporterMock = new ReporterMock();
    Reporter reporter = reporterMock;

    for (TestData pair : pairs) {
      Schema pairSchema = Pair.getPairSchema(
          Schema.create(Schema.Type.STRING), TigrCreator.inputSchema());
      AvroCollectorMock<Pair<CharSequence, Object>> collectorMock =
          new AvroCollectorMock<Pair<CharSequence, Object>>(pairSchema);

      TigrCreator.TigrMapper mapper = new TigrCreator.TigrMapper();
      mapper.configure(job);

      try {
        mapper.map(pair.input, collectorMock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
      // Check the values are equal.
      assertEquals(1, collectorMock.data.size());
      Pair<CharSequence, Object> outPair = collectorMock.data.get(0);
      assertEquals(pair.key, outPair.key().toString());
      assertEquals(pair.value, outPair.value());
    }
  }
}
