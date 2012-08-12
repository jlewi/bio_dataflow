package contrail.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestAvroFileContentsIterator {
  /**
   * Write data to a set of files. Each file contains part of the data.
   *
   * This creates the input for the test.
   *
   * @param numFiles
   * @param data
   * @return
   */
  protected List<String> writeToFiles(int numFiles, List<Integer> data) {
    List<String> files = new ArrayList<String>();

    if (data.size() % numFiles != 0) {
      fail("Test not setup correctly. Number of items must be an integer " +
           "multiple of the number of files");
    }

    int itemsPerFile = data.size() / numFiles;

    Iterator<Integer> dataIterator = data.iterator();

    try {
      for (int fIndex = 0; fIndex < numFiles; ++fIndex) {
        File temp = File.createTempFile("temp", Long.toString(System.nanoTime()));

        files.add(temp.toString());
        Schema schema = Schema.create(Schema.Type.INT);

        DatumWriter<Integer> datumWriter =
            new SpecificDatumWriter<Integer>(schema);
        DataFileWriter<Integer> writer =
            new DataFileWriter<Integer>(datumWriter);

        writer.create(schema, temp);
        for (int i = 0;  i < itemsPerFile; ++i) {
          writer.append(dataIterator.next());
        }
        writer.close();
      }

    } catch (IOException exception) {
      fail("Could not create temporary file. Exception:" +
          exception.getMessage());
    }
    return files;
  }

  @Test
  public void testItertor() {
    Random generator = new Random();
    // Create some random numbers.
    int numFiles = 3;
    int itemsPerFile = 10;
    ArrayList<Integer> expectedItems = new ArrayList<Integer>();
    for (int i = 0; i < numFiles * itemsPerFile; ++i) {
      expectedItems.add(generator.nextInt());
    }

    List<String> files = writeToFiles(numFiles, expectedItems);

    AvroFileContentsIterator<Integer> iterator =
        new AvroFileContentsIterator<Integer>(files, new Configuration());

    ArrayList<Integer> actualItems = new ArrayList<Integer>();

    while (iterator.hasNext()) {
      actualItems.add(iterator.next());
    }

    assertEquals(expectedItems, actualItems);
  }
}
