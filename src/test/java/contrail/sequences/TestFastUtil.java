package contrail.sequences;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

public class TestFastUtil {

  @Test
  public void testWriteFastQRecord() {
    FastQRecord record = new FastQRecord();
    record.setId("Some read");
    record.setRead("ACTTTA");
    record.setQvalue("!@!!!!");
    ByteArrayOutputStream outStream = new ByteArrayOutputStream();
    PrintStream stream = new PrintStream(outStream);
    FastUtil.writeFastQRecord(stream, record);
    stream.close();
    String actualValue = outStream.toString();
    String expectedValue =
        String.format(
            "@%s\n%s\n+\n%s\n", record.getId(), record.getRead(),
            record.getQvalue());

    assertEquals(expectedValue, actualValue);
  }

}
