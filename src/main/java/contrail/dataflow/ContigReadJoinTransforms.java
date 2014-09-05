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

import contrail.dataflow.JoinMappingsAndReads.JoinMappingReadDoFn;

/**
 * Transforms for joining the contigs, bowtie alignments, and reads.
 */
public class ContigReadJoinTransforms {
	public static class JoinMappingsAndReadsTransform extends 
			PTransform <PCollectionTuple, PCollection<ContigReadAlignment>> {
			
		      //PCollection<BowtieMapping> mappings, PCollection<Read> reads) {
		    PCollection<KV<String, BowtieMapping>> keyedMappings =
		        mappings.apply(ParDo.of(new BowtieMappingTransforms.KeyByReadId()))
		        .setCoder(KvCoder.of(
		            StringUtf8Coder.of(),
		            AvroSpecificCoder.of(BowtieMapping.class)));

		    PCollection<KV<String, Read>> keyedReads =
		        reads.apply(ParDo.of(new ReadTransforms.KeyByReadIdDo())).setCoder(
		            KvCoder.of(
		                StringUtf8Coder.of(),
		                AvroSpecificCoder.of(Read.class)));


		    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
		        KeyedPCollectionTuple.of(mappingTag, keyedMappings)
		        .and(readTag, keyedReads)
		        .apply(CoGroupByKey.<String>create());


		    PCollection<ContigReadAlignment> joined =
		        coGbkResultCollection.apply(ParDo.of(new JoinMappingReadDoFn(
		            mappingTag, readTag)));

		    return joined;
		  }
}
