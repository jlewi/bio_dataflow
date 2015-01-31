/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package contrail.dataflow;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Test;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder.Context;
import com.google.cloud.dataflow.sdk.coders.CoderException;

import contrail.scaffolding.BowtieMapping;
import contrail.sequences.FastQRecord;
import contrail.sequences.Read;

public class TestAvroSpecificCoder {
  private BowtieMapping emptyMapping() {
    BowtieMapping mapping = new BowtieMapping();
    mapping.setContigId("");
    mapping.setContigStart(0);
    mapping.setContigEnd(0);
    mapping.setNumMismatches(0);
    mapping.setRead("");
    mapping.setReadClearEnd(0);
    mapping.setReadClearStart(0);
    mapping.setReadId("");

    return mapping;
  }

  @Test
  public void testNested() throws CoderException, IOException {
    // To ensure the coder will work as a nested coder we serialize two
    // different schemas in sequence to the same stream. This test will
    // fail if the coder advances the stream beyond the end of the record.
    BowtieMapping mapping = emptyMapping();
    mapping.setReadId("readA");
    Context c;

    Read readA = new Read();
    readA.setFastq(new FastQRecord());
    readA.getFastq().setId("readA");
    readA.getFastq().setQvalue("");
    readA.getFastq().setRead("ACTCG");

    ByteArrayOutputStream outStream = new ByteArrayOutputStream();

    Context context = Context.NESTED;

    AvroCoder<BowtieMapping> mappingCoder = AvroCoder.of(
        BowtieMapping.class);
    AvroCoder<Read> readCoder = AvroCoder.of(
        Read.class);

    mappingCoder.encode(mapping, outStream, context);
    readCoder.encode(readA, outStream, context);

    ByteArrayInputStream inStream = new ByteArrayInputStream(
        outStream.toByteArray());

    BowtieMapping decodedMapping = mappingCoder.decode(inStream, context);
    Read decodedRead = readCoder.decode(inStream, context);
    assertEquals(mapping, decodedMapping);
    assertEquals(readA, decodedRead);
  }
}
