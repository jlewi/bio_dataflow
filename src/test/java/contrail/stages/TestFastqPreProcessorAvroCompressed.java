package contrail.stages;

import contrail.CompressedRead;
import contrail.ContrailConfig;
import contrail.OutputCollectorMock;
import contrail.ReporterMock;
import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.Sequence;
import contrail.util.ByteUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.avro.mapred.AvroWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.junit.Test;

public class TestFastqPreProcessorAvroCompressed {


  /**
   * Check that the mapper correctly processes the input.
   */
  private void processFrag(FastqPreprocessorAvroCompressed.FastqPreprocessorMapper mapper,
                           OutputCollectorMock<AvroWrapper<CompressedRead>, NullWritable> collector_mock,
                           Reporter reporter,
                           String[] lines) {
    OutputCollector<AvroWrapper<CompressedRead>, NullWritable> collector = collector_mock;
    Text map_value = new Text();
    LongWritable key = new LongWritable();
    Alphabet dnaalphabet = DNAAlphabetFactory.create();

    for (int i = 0; i < 4; i++) {
      map_value.set(lines[i]);
      key.set(i);
      try {
        mapper.map(key, map_value, collector, reporter);
      }
      catch (java.io.IOException e) {
        fail ("io exception in map:" + e.getMessage());
      }
    }
    CompressedRead read = collector_mock.key.datum();

    // Compute what the id should be.
    String true_id = lines[0];
    true_id = true_id.replaceAll("[:#-.|/]", "_");
    // chop the leading "@"
    true_id = true_id.substring(1);

    assertEquals(read.getId().toString(), true_id);

    // Make sure the array of packed bytes has the correct size.
    int buffer_size = read.dna.limit() - read.dna.arrayOffset();
    int expected_buffer_size = (int)Math.ceil((lines[1].length()* dnaalphabet.bitsPerLetter())/8.0);
    assertEquals(buffer_size, expected_buffer_size);

    Sequence sequence = new Sequence(dnaalphabet);
    sequence.readPackedBytes(read.dna.array(), read.length);

    assertEquals(lines[1].length(), sequence.size());

    for (int i=0; i < sequence.size(); i++) {
      assertEquals(sequence.at(i), lines[1].charAt(i));
    }
  }

  @Test
  public void TestMap() {
    OutputCollectorMock<AvroWrapper<CompressedRead>, NullWritable> collector_mock = new OutputCollectorMock<AvroWrapper<CompressedRead>, NullWritable>();


    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;

    FastqPreprocessorAvroCompressed.FastqPreprocessorMapper mapper = new FastqPreprocessorAvroCompressed.FastqPreprocessorMapper();

    // Configure it
    ContrailConfig.PREPROCESS_SUFFIX = 0;
    ContrailConfig.TEST_MODE = true;

    JobConf job = new JobConf(FastqPreprocessorAvroCompressed.FastqPreprocessorMapper.class);
    ContrailConfig.initializeConfiguration(job);
    mapper.configure(job);

    // Test some actua data from a fastq file.
    String[] frag1 = {"@gi|49175990|ref|NC_000913.2|_6741_6951_0:0:0_1:0:0_0/1",
                      "GCGGAAGGCTTAGCAAGGTGCCGCCGATGACCGTTG",
                      "+",
                      "222222222222222222222222222222222222"};

    String[] frag2 = {"@gi|49175990|ref|NC_000913.2|_3364_3577_1:0:0_3:0:0_143/1",
                      "GATACAGCTCGCTACCGCGCCGATTTGCGAGACCGC",
                      "+",
                      "222222222222222222222222222222222222"};

    String[] frag3 = {"@gi|49175990|ref|NC_000913.2|_3984_4189_1:0:0_0:0:0_1bc/1",
                      "CTCCGGGCGCCAATGTTGAAAGCGATGTCGGTTGTC",
                      "+",
                      "222222222222222222222222222222222222"};

    processFrag(mapper, collector_mock, reporter, frag1);
    processFrag(mapper, collector_mock, reporter, frag2);
    processFrag(mapper, collector_mock, reporter, frag3);

    // Generate some random fragments of various lengths.
    int max_length = 100;
    for (int frag_num = 0;  frag_num < 30; frag_num++) {
      // Generate the sequence label by randomly generating integers which correspond
      // to the printable utf8 characters 32 to 126
      int range = 126-32;
      int start = 32;


      byte[] label_bytes = new byte[(int)Math.ceil(Math.random()* max_length)];

      for (int pos = 0; pos < label_bytes.length; pos++) {
        label_bytes[pos] = ByteUtil.uintToByte((int)Math.ceil(Math.random()*range+start));
      }

      String label = ByteUtil.bytesToString(label_bytes, "UTF-8");
      label = "@" + label;

      // Generate the random sequence
      int sequence_length = (int)Math.ceil(Math.random()*max_length);
      String dna = "";

      Alphabet alphabet = DNAAlphabetFactory.create();
      for (int pos = 0; pos < sequence_length; pos++) {
        dna += alphabet.validChars()[(int)Math.floor(Math.random()*alphabet.validChars().length)];
      }

      String[] frag = {label, dna, "+", "222222"};
      processFrag(mapper, collector_mock, reporter, frag);
    }
  }
}
