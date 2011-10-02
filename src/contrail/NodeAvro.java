package contrail;

import java.lang.RuntimeException;
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
 * 
 */
public class NodeAvro extends NodeBase {
	
	// Instance of the wrapper for the avro data structure representing
	// the data
	private NodeData data = new NodeData();
	
	/**
	 * Serialize the node to a string.
	 *
	 * @param tagfirst - If true, then the first field is nodeid_m.
	 * 
	 * The node is serialized is serialized as a tab delimited sequence of fields.
	 * Furthermore the '*' character is used as a separation field between fields.
	 * Each field is encoded as a pair
	 * 	  *FIELD_ID\tFIELD_VALUE
	 * where FIELD_ID is a string identifying the field and FIELD_VALUE is the value.
	 * For some fields (e.g the fields corresponding to the edge types) FIELD_VALUE can
	 * be a list of values in which case the items are tab delimeted. e.g
	 *    *FIELD_ID\tVALUE1\tVALUE2\tVALUE3
	 * 
	 * If tagfirst is true then the first value is nodeid_m.
	 * then  
	 * @return
	 */
	public String toNodeMsg(boolean tagfirst)
	{
		throw new RuntimeException("Not implemented");
		//return "";
	}
	
	public NodeAvro(String nodeid)
	{
		nodeid_m = nodeid;
	}
	
	public NodeAvro()
	{
		data.mertag="";
	}
	
	public NodeAvro(NodeData data){
		this.data=data;
	}
	// Iterator over the list of values associated with a field.
	protected Iterator<?> fieldValuesIterator(String field) {
		return data.fields.get(field).iterator();
	}
	/**
	 * Copy Constructor.
	 */
	public NodeAvro(NodeBase other){
			
		this.nodeid_m=other.nodeid_m;
		data.fields = new java.util.HashMap<java.lang.CharSequence,java.util.List<java.lang.CharSequence>>();
		data.mertag="";
		Iterator<?> key_it= other.fieldsIterator(); 
		while (key_it.hasNext()){
			String key= (String) key_it.next();
			ArrayList<java.lang.CharSequence> items = new ArrayList<java.lang.CharSequence>();
			
			Iterator<?> item_it = other.fieldValuesIterator(key);		
			while (item_it.hasNext()){
				items.add((java.lang.CharSequence) item_it.next());
			}
			data.fields.put(key,items);
		}
		
	}
	
		
	public NodeData getData(){
		return data;
	}
		
	protected Iterator<?> fieldsIterator(){
		return data.fields.keySet().iterator();
	}
	
	/**
	 * Return a list of strings for the specified field.
	 * 
	 * If the field doesn't exist then a new list of strings is initialized
	 * and returned.
	 * 
	 * @param field - Name of the field
	 * @return - List of strings to represent this field. 
	 */
//	protected List<java.lang.CharSequence> getOrAddField(String field)
//	{
//		 
//		
//		if (data.fields.containsKey(field))
//		{
//			return data.fields.get(field);
//		}
//				
//		List<java.lang.CharSequence> retval = new ArrayList<java.lang.CharSequence>();
//		data.fields.put(field, retval);
//		
//		return retval;
//	}
	
	public void setMertag(String tag)
	{		
		data.mertag=tag;
	}
	
	protected boolean hasField (String field){
		return data.fields.containsKey(field);
	}

}
