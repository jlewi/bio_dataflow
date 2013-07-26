/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Author: Jeremy Lewi (jeremy@lewi.us)
package contrail.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 * Schema for a bigquery record.
 *
 * A schema is basically a collection of fields.
 */
public class BigQuerySchema extends ArrayList<BigQueryField> {
  @Override
  public String toString() {
    return toJson();
  }

  /**
   * Returns the json representation that BigQuery expects for schemas.
   * @return
   */
  public String toJson() {
    // Only include non null fields.
    ObjectMapper mapper = new ObjectMapper();

    mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);

    // Avoid printing empty lists. Its not clear whether that cause problems
    // with the bigquery api.
    mapper.getSerializationConfig().setSerializationInclusion(JsonSerialize.Inclusion.NON_DEFAULT);
    String json = "";
    try {
      json = mapper.writeValueAsString(this);
    } catch (JsonGenerationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (JsonMappingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    return json;
  }

  /**
   * Construct the schema from a schema object.
   * @param schema
   * @return
   */
  public static BigQuerySchema fromAvroSchema(Schema schema) {
    BigQuerySchema bqSchema = new BigQuerySchema();

    if (schema.getType() != Type.RECORD) {
      throw new RuntimeException("Avro schema must be a record.");
    }

    for (Field field : schema.getFields()) {
      // TODO(jeremy@lewi.us): Will we get data loss if we represent Avro
      // longs as big query integers? Big query only has the integer type.
      if (field.schema().getType() == Type.INT ||
          field.schema().getType() == Type.LONG) {
        bqSchema.add(new BigQueryField(field.name(), "integer"));
      } else if (field.schema().getType() == Type.STRING) {
        bqSchema.add(new BigQueryField(field.name(), "string"));
      } else if (field.schema().getType() == Type.BOOLEAN) {
        bqSchema.add(new BigQueryField(field.name(), "boolean"));
      } else if (field.schema().getType() == Type.FLOAT ||
                 field.schema().getType() == Type.DOUBLE) {
        bqSchema.add(new BigQueryField(field.name(), "float"));
      } else if (field.schema().getType() == Type.RECORD) {
        BigQuerySchema subRecord = BigQuerySchema.fromAvroSchema(
            field.schema());
        BigQueryField subField = new BigQueryField(field.name(), "record");
        subField.fields.addAll(subRecord);
        bqSchema.add(subField);
      } else if (field.schema().getType() == Type.UNION) {
        // The json encoder for a union just outputs a record. The field names
        // are the fully qualified schema names. Only the field for the item
        // actually represented in the schema is set.
        List<Field> unionFields = new ArrayList<Field>();
        for (Schema item : field.schema().getTypes()) {
          if (item.getType() == Type.NULL) {
            continue;
          }
          // JsonEncoder uses the fully qualified name of the schema as the
          // field name. This creates problems for BigQuery so we only use
          // the final part of the name. This is compatible with
          // BigQueryJsonEncoder. There is a small possibility that we could
          // have problems if we have two schemas with the same name in
          // different namespaces appearing in the union.
          Field avroField = new Field(item.getName(), item, null, null);
          unionFields.add(avroField);
        }
        Schema recordSchema = Schema.createRecord(unionFields);
        BigQueryField bigQueryField = new BigQueryField(
            field.name(), recordSchema);
        for (BigQueryField subField : bigQueryField.fields) {
          subField.mode = "nullable";
        }
        bqSchema.add(bigQueryField);
      } else {
        throw new RuntimeException(
            "We don't know how to handle the avro schema:" +
                field.schema().toString());
      }
    }
    return bqSchema;
  }
}
