package contrail;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

/**
 * The node class represents a node in the graph.
 * Each node represents a k-mer and edges are drawn
 * between two nodes if they overlap by k-1 bases.
 * 
 * Each node contains a bunch of fields stored in a hash
 * table. The value of each field is a list of strings.
 * 
 * Each node stores a list of out going edges. The out 
 * going edges are divided into four groups:
 *  "ff", "fr", "rf","rr"
 * where each group corresponds to a different edge type.
 * The edge type is determined by the canonical direction
 * of the kmer (see BuildGraph.BuildGraphMapper.Map)  
 * 
 * Subclasses are responsible for customizing the serialization and internal data structures.
 * 
 * TODO(jlewi): Should this be an interface instead of a base class? i.e Define
 * an interface for Serialization. 
 */
public abstract class NodeBase {
		
	static String [] dnachars  = {"A", "C", "G", "T"};
	static Map<String, String> str2dna_ = initializeSTR2DNA(); 
	static Map<String, String> dna2str_ = initializeDNA2STR();
		
	// Id for the node. 
	protected String nodeid_m;
			
	// Methods which need to be overwritten in sub class
	protected abstract Iterator<?> fieldsIterator() ;
				
	// Iterator over the list of values associated with a field.
	protected abstract Iterator<?> fieldValuesIterator(String field);
	
	// converts strings like A, GA, TAT, ACGT to compressed DNA codes (A,B,C,...,HA,HB)
	protected static Map<String,String> initializeSTR2DNA()
	{
	   int num = 0;
	   int asciibase = 'A';
	   
	   Map<String, String> retval = new HashMap<String, String>();
	     
	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   retval.put(dnachars[xi], 
				      Character.toString((char) (num + asciibase)));
		   
		   num++;
		   
		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   retval.put(dnachars[xi] + dnachars[yi], 
					      Character.toString((char) (num + asciibase)));   
			   num++;
		   }
	   }
	   
	   for (int xi = 0; xi < dnachars.length; xi++)
	   {		   
		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   String m = retval.get(dnachars[xi] + dnachars[yi]);
			   
			   for (int zi = 0; zi < dnachars.length; zi++)
			   {
				   retval.put(dnachars[xi]+dnachars[yi]+dnachars[zi],
						      m + retval.get(dnachars[zi]));
				   
				   for (int wi = 0; wi < dnachars.length; wi++)
				   {
					   retval.put(dnachars[xi]+dnachars[yi]+dnachars[zi]+dnachars[wi],
							      m+retval.get(dnachars[zi]+dnachars[wi]));
				   }
			   }
		   }
	   }
	   
	   return retval;
	}
	
	// converts single letter dna codes (A,B,C,D,E...) to strings (A,AA,GT,GA...)
	protected static Map<String, String> initializeDNA2STR()
	{
	   int num = 0;
	   int asciibase = 65;
	   
	   Map<String, String> retval = new HashMap<String, String>();
	   
	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   retval.put(Character.toString((char) (num + asciibase)),
				      dnachars[xi]);
		   
		   num++;
		   
		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   retval.put(Character.toString((char) (num + asciibase)),
					      dnachars[xi] + dnachars[yi]); 
			   num++;
		   }
	   }
	   
	   /*
	   Set<String> keys = retval.keySet();
	   Iterator<String> it = keys.iterator();
	   while(it.hasNext())
	   {
		   String k = it.next();
		   String v = retval.get(k);
		   
		   System.err.println(k + "\t" + v);
	   }
	   */
	   
	   return retval;
	}

	// converts a tight encoding to a normal ascii string
	public static String dna2str(String dna)
	{	
		StringBuffer sb = new StringBuffer();
		
		for (int i = 0; i < dna.length(); i++)
		{
			sb.append(dna2str_.get(dna.substring(i,i+1)));
		}
		
		return sb.toString();
	}
	
	/**
	 * Convert a DNA sequence to a compressed form.
	 * 
	 * We read in sequences of 4 bases and convert them to a compact encoding.
	 * 
	 * @param seq
	 * @return - The compressed sequence. 
	 */
	public static String str2dna(String seq)
	{
		StringBuffer sb = new StringBuffer();
		
	    int l = seq.length();
	    
	    int offset = 0;
	    
	    while (offset < l)
	    {
	    	int r = l - offset;
	    	
	    	if (r >= 4) 
	    	{ 
	    		sb.append(str2dna_.get(seq.substring(offset, offset+4))); 
	    		offset += 4; 
	    	}
	    	else 
	    	{ 
	    		sb.append(str2dna_.get(seq.substring(offset, offset+r)));
	    		offset += r;
	    	}
	    }

        return sb.toString();
	}
			
	public abstract void setMertag(String tag);	
	protected abstract boolean hasField(String field);
	
	public String getNodeId() { return nodeid_m; }
	public void setNodeId(String nid) { nodeid_m = nid; }
	
	public NodeBase(String nodeid)
	{
		nodeid_m = nodeid;
	}
	
	public NodeBase()
	{
		
	}

}
