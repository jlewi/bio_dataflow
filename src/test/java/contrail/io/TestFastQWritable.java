package contrail.io;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class TestFastQWritable {
  @Test
  public void testRead() {
    String inRecord = "@SRR1\nGCT\n+\n!!2";

    ByteArrayInputStream inBytes = new ByteArrayInputStream(
        inRecord.getBytes());
    DataInputStream inStream = new DataInputStream(inBytes);

    FastQWritable record = new FastQWritable();
    try {
      record.readFields(inStream);
      inStream.close();
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem reading the data:" + e.getMessage());
    }

    assertEquals("SRR1", record.getId());
    assertEquals("GCT", record.getDNA());
    assertEquals("!!2", record.getQValue());
  }

  @Test
  public void testWrite() {
    FastQWritable record = new FastQWritable();
    record.setId("SRR1");
    record.setDNA("GCT");
    record.setQValue("!!2");

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream outStream = new DataOutputStream(byteStream);

    try {
      record.write(outStream);
      outStream.close();
    } catch (IOException e) {
      throw new RuntimeException(
          "There was a problem writing the data:" + e.getMessage());
    }

    String output = byteStream.toString();

    String expectedOutput =
        "@" + record.getId() + "\n" + record.getDNA() + "\n+\n" +
        record.getQValue() + "\n";
    assertEquals(expectedOutput, output);
  }
}
