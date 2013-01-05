package contrail.correct;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

/** Creates a FastQRecord. 
 * This record reader reads a 4 line fastq record
 * and passes it to the mapper as FastQText which
 * is a subclass of the Hadoop Text type.
 */
@SuppressWarnings("unused")
class FastQRecordReader implements RecordReader<LongWritable, FastQText> {
  private static final Logger sLogger 
  = Logger.getLogger(FastQRecordReader.class);

  //The constructor gets this as a parameter. This data member stores it in a
  //private member so it can be used to get the number in the getSplitsForFile
  //method.

  private NumberedFileSplit  fileSplit;
  private long               start;
  private long               pos;
  private long               end;
  private LineReader         lr;
  private FSDataInputStream  fileIn; 
  int                        maxLineLength;

  // Created this data member because this is defined in the newer api's
  // 'mapreduce' package which is somehow not accessible from here.

  public static final String MAX_LINE_LENGTH = 
      "mapreduce.input.linerecordreader.line.maxlength";

  public FastQRecordReader(Configuration job, NumberedFileSplit split)
      throws IOException {
    this(job, split, null);
    fileSplit = split;
  }

  @SuppressWarnings("deprecation")
  public FastQRecordReader(Configuration job, NumberedFileSplit split,
      byte[] recordDelimiter) throws IOException {

    fileSplit = split;
    this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    
    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(job);
    fileIn = fs.open(file);
    
    fileIn.seek(start);
    lr = new LineReader(fileIn);

    this.pos = start;
  }
  
  public long getpos()
  {
    return pos; 
  }
  
  public LongWritable createKey() {
    return new LongWritable();
  }

  public FastQText createValue() {
    return new FastQText();
  }
  @Override
  public synchronized boolean next(LongWritable key, FastQText value)
      throws IOException {
  
    Text[] record = new Text[4];
    
    for (int i = 0;i<4; i++)
      record[i] = new Text();
    
    // We never read past the split boundary.
    
    while (pos < end) {
      
      //System.out.println("pos: "+ pos + "end:"+ end);
      key.set(fileSplit.getNumber());
      for (int i = 0; i< 4; i++)
      {
        int newSize = lr.readLine(record[i]);
               
        if (newSize == 0 && i == 0)
        {
          return false;
        }
        
        if (newSize == 0 && i>0)
        {
          sLogger.info("[ERROR]: Malformed Data");
          System.out.println("Malformed Data");
          throw new IOException("ERROR: Malformed Data");
        }      
        pos = pos + newSize;        
      }    
      
      value.set(record[0].toString(), record[1].toString(),
          record[3].toString());
      
      return true;
    }  
    return false;
  }

  /**
   * Get the progress within the split
   */
  
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  public  synchronized long getPos() throws IOException {
    return pos;
  }
  public synchronized void close() throws IOException {
    if (lr != null) {
      lr.close(); 
    }
  }
}
