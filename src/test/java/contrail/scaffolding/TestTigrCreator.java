package contrail.scaffolding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;

import contrail.OutputCollectorMock;
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
  public void testMapper() {
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

  public class ReducerData {
    public AvroKey<CharSequence> inputKey;
    public ArrayList<AvroValue<Object>> inputValues;
    public ArrayList<String> outputs;

    public ReducerData() {
      inputValues = new ArrayList<AvroValue<Object>>();
      outputs = new ArrayList<String>();
    }
  }

  private void runReduceTest(ReducerData data) {
    JobConf job = new JobConf(TestTigrCreator.class);
    ReporterMock reporterMock = new ReporterMock();
    Reporter reporter = reporterMock;
    Schema pairSchema = Pair.getPairSchema(
        Schema.create(Schema.Type.STRING), TigrCreator.inputSchema());
    OutputCollectorMock<Text, NullWritable> collectorMock =
        new OutputCollectorMock<Text, NullWritable> (
            Text.class, NullWritable.class);

    TigrCreator.TigrReducer reducer = new TigrCreator.TigrReducer();
    reducer.configure(job);

    try {
      reducer.reduce(
          data.inputKey, data.inputValues.iterator(), collectorMock, reporter);
    }
    catch (IOException exception){
      fail("IOException occured in map: " + exception.getMessage());
    }

    // Check the values are equal.
    assertEquals(data.outputs.size(), collectorMock.outputs.size());

    for (int i = 0; i < collectorMock.outputs.size(); ++i) {
      assertEquals(
         data.outputs.get(i), collectorMock.outputs.get(i).key.toString());
    }
  }

  @Test
  public void testReducerForward() {
    // This test covers the case where the read is aligned to
    // the forward strand of the contig.
    ReducerData data = new ReducerData();

    String contigId = "contig";
    String sequence = "ACTGGGGAACCCTTT";
    data.inputKey = new AvroKey<CharSequence>(contigId);
    {
      GraphNode node = new GraphNode();
      node.setNodeId(contigId);
      node.setSequence(new Sequence(sequence, DNAAlphabetFactory.create()));
      data.inputValues.add(
          new AvroValue<Object>(node.clone().getData()));
    }

    String read;
    String readId = "read_2_0_75";
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping = new BowtieMapping();
      mapping.setContigId(contigId);
      mapping.setReadId(readId);
      mapping.setContigStart(2);
      mapping.setContigEnd(8);
      mapping.setReadClearStart(0);
      mapping.setReadClearEnd(6);
      mapping.setNumMismatches(0);

      read = sequence.substring(2, 9);
      mapping.setRead(read);

      data.inputValues.add(new AvroValue<Object>(mapping));
    }

    data.outputs.add(String.format(
        "##%s 1 %d bases, 00000000 checksum.", contigId, sequence.length()));
    data.outputs.add(sequence);
    data.outputs.add(String.format(
        "#%s(2) [] 7 bases, 00000000 checksum. {1 7} <3 9>", readId));

    runReduceTest(data);
  }

  @Test
  public void testReducerReverse() {
    // This test covers the case where the read is aligned to
    // the reverse strand of the contig.
    ReducerData data = new ReducerData();

    String contigId = "contig";
    String sequence = "ACTGGGGAACCCTTT";
    data.inputKey = new AvroKey<CharSequence>(contigId);
    {
      GraphNode node = new GraphNode();
      node.setNodeId(contigId);
      node.setSequence(new Sequence(sequence, DNAAlphabetFactory.create()));
      data.inputValues.add(
          new AvroValue<Object>(node.clone().getData()));
    }

    String read;
    String readId = "read_2_0_75";
    {
      BowtieMapping mapping = new BowtieMapping();
      mapping = new BowtieMapping();
      mapping.setContigId(contigId);
      mapping.setReadId(readId);
      mapping.setContigStart(8);
      mapping.setContigEnd(2);
      mapping.setReadClearStart(0);
      mapping.setReadClearEnd(6);
      mapping.setNumMismatches(0);

      // Bowtie will have reverse complemented the read so it matches
      // the forward strand of the contig.
      read = sequence.substring(2, 9);
      mapping.setRead(read);

      data.inputValues.add(new AvroValue<Object>(mapping));
    }

    data.outputs.add(String.format(
        "##%s 1 %d bases, 00000000 checksum.", contigId, sequence.length()));
    data.outputs.add(sequence);
    data.outputs.add(String.format(
        "#%s(2) [RC] 7 bases, 00000000 checksum. {7 1} <3 9>", readId));

    runReduceTest(data);
  }
}
