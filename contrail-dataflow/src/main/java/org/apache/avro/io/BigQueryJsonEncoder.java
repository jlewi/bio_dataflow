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
package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonGenerator;

/**
 * Customize the json encoder to produce BigQuery compatible json.
 *
 * This class needs to be in package org.apache.avro.io because the JsonEncoder
 * constructors are only package visible.
 */
public class BigQueryJsonEncoder extends JsonEncoder {
  // The member variable out is private so we need to steal a reference
  // so that we can access it.
  protected JsonGenerator jsonGenerator;
  public BigQueryJsonEncoder(Schema sc, OutputStream out) throws IOException {
    super(sc, out);
  }

  public BigQueryJsonEncoder(Schema sc, JsonGenerator out) throws IOException {
    super(sc, out);
  }

  @Override
  public JsonEncoder configure(JsonGenerator generator) throws IOException {
    super.configure(generator);
    // The member variable out is private so we need to steal a reference
    // so that we can access it.
    jsonGenerator = generator;
    return this;
  }

  @Override
  public void writeIndex(int unionIndex) throws IOException {
    parser.advance(Symbol.UNION);
    Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
    Symbol symbol = top.getSymbol(unionIndex);
    if (symbol != Symbol.NULL) {
      jsonGenerator.writeStartObject();
      // BigQuery can't handle names with "." so we only use the final
      // part of the name.
      String label = top.getLabel(unionIndex);
      String pieces[] = StringUtils.split(label, ".");
      String name = pieces[pieces.length - 1];
      jsonGenerator.writeFieldName(name);
      parser.pushSymbol(Symbol.UNION_END);
    }
    parser.pushSymbol(symbol);
  }
}
