package contrail.correct;

import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;

/** Specialized SubClass to hold FastQRecord
 * @author deepak
 */

public class FastQText extends Text {
  public static char separator = '\n';

  public FastQText() {
    super();
  }
  
  public FastQText(String id, String dna, String qvalue) {
    super(join(id, dna, qvalue));
  }
  
  public void set(String id, String dna, String qvalue) {
    super.set(join(id, dna, qvalue));
  }
  public static String join (String id, String dna, String qvalue) {
    return id + "\n" + dna + "\n" + qvalue;
  }
  
  public String toString()
  {
    return super.toString();    
  }
  
  // get Sequence ID.
  public String getId()
  {
    StringTokenizer st = new StringTokenizer(toString(), "\n");
    return st.nextToken();
  }

  //Get DNA Sequence
  public String getDna()
  {
    StringTokenizer st = new StringTokenizer(toString(), "\n");
    st.nextToken();
    return st.nextToken();
    
  }
  
  //Get Quality Value.
  public String getQValue()
  {
    StringTokenizer st = new StringTokenizer(toString(), "\n");
    st.nextToken();
    st.nextToken();
    return st.nextToken();
  }
}
