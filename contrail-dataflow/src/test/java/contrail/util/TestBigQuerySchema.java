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

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.junit.Test;

import contrail.graph.LengthStatsData;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;

public class TestBigQuerySchema {
  @Test
  public void toJsonTest() {
    BigQuerySchema schema = new BigQuerySchema();

    schema.add(new BigQueryField("Kmer", "string"));
    schema.add(new BigQueryField("before", "integer"));
    schema.add(new BigQueryField("after", "integer"));

    String json = schema.toJson();

    System.out.println(json);
  }

  @Test
  public void fromAvroSchema() {
    LengthStatsData statsData = new LengthStatsData();
    BigQuerySchema schema = BigQuerySchema.fromAvroSchema(
        statsData.getSchema());

    String json = schema.toJson();
    System.out.println(json);
  }

  @Test
  public void fromAvroSchemaUnionNull() {
    // Test when the schema is a union with null and a record.
    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(Schema.create(Type.NULL));
    schemas.add((new FastQRecord()).getSchema());
    Schema unionSchema = Schema.createUnion(schemas);

    List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("union_field", unionSchema, null, null));
    Schema recordSchema = Schema.createRecord(fields);
    BigQuerySchema schema = BigQuerySchema.fromAvroSchema(recordSchema);

    String json = schema.toJson();
    System.out.println(json);
  }

  @Test
  public void fromAvroSchemaNested() {
    // Test when the schema includes nested records.
    Read read = new Read();
    BigQuerySchema schema = BigQuerySchema.fromAvroSchema(
        read.getSchema());

    String json = schema.toJson();
    System.out.println(json);
  }
}
