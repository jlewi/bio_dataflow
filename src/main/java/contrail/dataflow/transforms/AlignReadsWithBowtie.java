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

/**
 * A transform for aligning reads with bowtie.
 * 
 * input: A tuple of pcollection containing the reads and contigs.
 * output: A pcollection of bowtie alignments.
 */
public class AlignReadsWithBowtie extends PTransform<PCollectionTuple, PCollection<BowtieAlignment>> {  
	final public TupleTag<Read> readTag = new TupleTag<>();
	final public TupleTag<GraphNodeData> nodeTag = new TupleTag<>();

	private int shortenedLength;

	private static class ShortenReans extends DoFn<Read, Read> {
	  
	  private int shortenedLength;
	  
	  public ShortenReads(int shortenedLength) {
	    this.shortenedLengthed = shortenedLength;
	  }
	  
	  public List<String> shorten(String sequence) {
	    List<String> pieces = new ArrayList<String>();	      
      if (readLength < shortenedLength) {
        return pieces;
      }
      
      // Get the first part
      pieces.add(sequence.subSequence().subSequence(0, shortenedLength));
      // Get the end.
      pieces.add(sequence.subSequence(sequence.size() - shortenedLength);
      
      // Get the middle part.
      int start = floor(sequence.size()/2.0 - shortenedLength/2.0);
      pieces.add(sequence.subSequence(start, start + shortenedLength));      
	  }
	  
	  @Override
    public void processElement(ProcessContext c) {
	    Read read = c.element();
	    
	    
	    if (read.hasFasta()) {
	      Fasta fasta = read.getFasta();
	      String sequence = fasta.getFasta().;
	      List<String> pieces = shorten(sequence);
	      for (String subSequence : pieces) {
	        Read shortRead = new Read();
	        read.
	      }
	    }
	  }
	}
	public AlignReadsWithBowtie(int shortenedLength) {
	  this.shortenedLength = shortenedLength;
	}
	
	public apply(PCollectionTuple inputTuple)) {
		PCollection<Read> reads = inputTuple.get(readTag);
		PCollection<GraphNodeData> nodes = inputTuple.get(nodeTag); 
		
	  PCollection<Read> shortenedReads = reads.apply(ParDo.of(new ShortenReads(shortenedLength)));
	  
	  // Build the index and copy it to GCS.
	  PObject<IndexInfo> index = nodes.apply(SeqDo.of(BuildIndex()));
	  
	  // Align the reads.
	  PCollection<BowtieAlignment) aligned = shortenedReads.apply(ParDo.withSideInput(new AlignReads(), index));
	
	  return aligned;
	}
}
