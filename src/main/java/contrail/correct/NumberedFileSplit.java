package contrail.correct;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;

/** NumberedFileSplit extends FileSplit. The only thing that it does differently 
 * from FileSplit is that is has a new member called "number" which stores 
 * information about what split 'number' is the current split.   
 */

class NumberedFileSplit extends FileSplit implements InputSplit {

  private Path file;
  private long start;
  private long length;
  // This gives us the
  private long number;
  private String[] hosts;
  public NumberedFileSplit() {
    // This is the only public constructor of FileSplit
    super((Path) null, 0, 0, (String[]) null);
  }
  // simply call super's constructor for initialization.
  public NumberedFileSplit(Path file, long start, long length, long number,
      String[] hosts) {
    super(file,start, length, hosts);
    this.file = file;
    this.start = start;
    this.length = length;
    this.hosts = hosts;
    this.number = number;
  }
  public long getNumber() {
    return number;
  }
  @Override
  public String toString() {
    return file + ":" + start + "+" + length + "+" + number;
  }

  public void write(DataOutput out) throws IOException {
    Text.writeString(out, file.toString());
    out.writeLong(start);
    out.writeLong(length);
    out.writeLong(number);
  }

  public void readFields(DataInput in) throws IOException {
    file = new Path(Text.readString(in));
    start = in.readLong();
    length = in.readLong();
    number = in.readLong();
    hosts = null;
  }

  /** The file containing this split's data. */
  public Path getPath() {
    return file;
  }

  /** The position of the first byte in the file to process. */
  public long getStart() {
    return start;
  }

  /** The number of bytes in the file to process. */
  @Override
  public long getLength() {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[] {};
    } else {
      return this.hosts;
    }
  }
}