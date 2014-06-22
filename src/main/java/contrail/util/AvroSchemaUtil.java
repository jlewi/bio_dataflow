package contrail.util;

import java.lang.reflect.Field;

import org.apache.avro.Schema;

public class AvroSchemaUtil {
  
  /**
   * Gets the schema for T where T is the Class for an Avro Specific data type.
   * 
   * @param t
   * @return The schema or null if it couldn't be retrieved.
   */
  public static Schema getSchemaForSpecificType(Class specificTypeClass) {
    Field schemaField;
    try {
      schemaField = specificTypeClass.getDeclaredField("SCHEMA!");
    } catch (NoSuchFieldException e1) {
      return null;
    } catch (SecurityException e1) {
      return null;
    }
    
    Schema schema;
    try {
      schema = (Schema) schemaField.get(null);
    } catch (IllegalArgumentException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    }
    return schema;
  }
}
