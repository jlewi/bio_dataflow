package contrail.correct;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.util.FileHelper;

public class TestConvertKMerCountsToText {
  @Test
  public void testRun() {
    File temp = FileHelper.createLocalTempDir();;
    temp.deleteOnExit();
    File avroFile = new File(temp, "input.avro");

    // Write the data to the file.
    Pair<CharSequence, Long> inPair = new Pair<CharSequence, Long>("ACT", 1);

    Schema schema = inPair.getSchema();
    DatumWriter<Pair<CharSequence, Long>> datumWriter =
        new SpecificDatumWriter<Pair<CharSequence, Long>>(schema);
    DataFileWriter<Pair<CharSequence, Long>> writer =
        new DataFileWriter<Pair<CharSequence, Long>>(datumWriter);

    Alphabet alphabet = DNAAlphabetFactory.create();
    Random generator = new Random();

    try {
      writer.create(schema, avroFile);
      for (int i = 0; i < 1; ++i) {
        inPair.key(AlphabetUtil.randomString(generator, 10, alphabet));
        inPair.value(generator.nextLong());
        writer.append(inPair);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing the graph to an avro file. Exception:" +
          exception.getMessage());
    }

    // Run it.
    ConvertKMerCountsToText stage = new ConvertKMerCountsToText();
    File outputPath = new File(temp, "output");

    String[] args =
      {"--inputpath=" + temp.toURI().toString(),
        "--outputpath=" + outputPath.toURI().toString()};

    try {
      stage.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}


