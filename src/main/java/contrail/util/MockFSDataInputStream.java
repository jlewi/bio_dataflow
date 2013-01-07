package contrail.util;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.util.ByteBufferInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

// utility class to mock FSDataInputStream.
// Uses a ByteBuffer underneath to support the interface of a FSDataInputStream
public class MockFSDataInputStream extends BufferedInputStream implements
Seekable, PositionedReadable {

  ByteBuffer src;

  public MockFSDataInputStream(ByteBuffer in) {
    super(new ByteBufferInputStream(getList(in)), in.limit());
    src = in;
  }

  private static List<ByteBuffer> getList(ByteBuffer in)
  {
    List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();
    bufferList.add(in);
    return bufferList;
  }

  public long getPos() throws IOException {
    return src.position();
  }

  public long skip(long n) throws IOException {
    
    if (n <= 0) {
      return 0;
    }
    src.position(src.position() + (int) n);
    return n;
  }

  public void seek(long pos) throws IOException {
    if (pos < 0) {
      return;
    }
    
    long end = src.position();
    long start = end - count;
    if (pos >= start && pos < end) {
      this.pos = (int) (pos - start);
      return;
    }

    this.pos = 0;
    this.count = 0;

    src.position((int) pos);
  }

  public boolean seekToNewSource(long targetPos) throws IOException {
    // Not Supporting this yet.
    return true;
  }

  
  public int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    src.mark();
    src.position((int) position);
    int bytesToRead = Math.min(src.remaining(), length);
    src.get(buffer, offset, bytesToRead);
    src.reset();
    return bytesToRead;
  }

  public void readFully(long position, byte[] buffer, int offset, int length)
      throws IOException {
    src.mark();
    src.position((int) position);
    src.get(buffer, offset, length);
    src.reset();
  }

  public void readFully(long position, byte[] buffer) throws IOException {
    src.mark();
    src.position((int) position);
    src.get(buffer);
    src.reset();
  }
}