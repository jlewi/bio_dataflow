/**
 * Licensed under the Apache License, Version 2.0 (the "License");
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
// Author: Avijit Gupta (mailforavijit@gmail.com)

package contrail.correct;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.Alphabet;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.Sequence;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * A bitvector is required by the quake engine during its correction stage.
 * The bitvector is encoded as an array of bytes where the i'th kmer correspond to
 * the i % 8  bit in the floor(i/8) byte.  The ith bit in the bitvector tells us whether the kmer
 * i= h(kmer) represents a valid kmer or an error. The function h is a hash function
 * it hashes kmer's to integers. We typically take kmers whose counts are below
 * the cutoff as untrusted whereas trusted kmers have counts greater than or
 * equal to the cutoff. The integer is the position of the kmer in the bitvector, ie.
 * a bit in the output vector at the position calculated by h(kmer)
 * indicating the status of that kmer.
 *
 * The mapper prunes the kmer count file according to the cutoff obtained
 * A kmer count file is a file that contains the number of times
 * a kmer occurs in the dataset. It is represented as a pair of <Kmer,count>
 * in avro format.  The cutoff is supplied as a parameter.
 *
 * The mapper reads in Avro records and emits out avro records if the value
 * of count is greater than cutoff.
 * The reducer builds the bitvector in a serial manner. Since the kmers that reach the reducer are sorted,
 * we can write out the bitvector in a serial manner either directly on a file, or within an
 * in memory store that is finally written out to a HDFS location. It is to be
 * noted that the bitvector isnt emitted from the reducer. Instead, we write it
 * directly onto the HDFS using the write() method from within reducer's close().
 *
 * Requirement:
 * The value of K should be positive and less that 20. This is because we maintain
 * an in memory hashMap which contains 4^K bits.Even if we remove this limitation,
 * the bitvector that is generated has to be used by quake for correction purposes.
 * Quake isn't able to handle bigger values of K since quake is serial and the
 * bitvector has to be brought into memory.
 */
public class BuildBitVector extends MRStage {
  private static final Logger sLogger = Logger.getLogger(BuildBitVector.class);
  // The name for the file to store the bitvector.
  public final static String VECTOR_FILENAME = "bitvector.binary";

  public static class FilterMapper extends AvroMapper<Pair<CharSequence, Long>, Pair<CharSequence, Long>> {
    private int cutOff;
    Sequence dnaSequence;

    public void configure(JobConf job){
      BuildBitVector stage = new BuildBitVector();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      cutOff = (Integer)(definitions.get("cutoff").parseJobConf(job));
      dnaSequence = new Sequence(DNAAlphabetFactory.create());
    }

    // incoming key,value pairs - (kmer, frequency)
    public void map(Pair<CharSequence, Long> countRecord,
        AvroCollector<Pair<CharSequence, Long>> output, Reporter reporter) throws IOException {
      long kmerFrequency = countRecord.value().longValue();
      if(kmerFrequency >= cutOff) {
        String kmer = countRecord.key().toString();
        dnaSequence.readCharSequence(kmer);
        String reverseComplement  = DNAUtil.reverseComplement(dnaSequence).toString();
        Pair<CharSequence, Long> reverseComplementCountRecord = new Pair<CharSequence, Long>(reverseComplement, kmerFrequency);
        // We emit both the kmer and its reverse complement so that both of them are set
        // in the bitvector
        output.collect(countRecord);
        output.collect(reverseComplementCountRecord);
      }
    }
  }

  /**
   * A single instance of this reducer is launched. This creates the bitvector, one byte at a time.
   * Every Kmer has an index in this bitvector, and the index is governed by the sorted order
   * in which the Kmer appears. We calculate the index of the incoming Kmer and compare it
   * to the current byte index. If the new kmer lies within the same byte (8 locations)
   * of the current byte, the the index of the new kmer is set on that byte. Otherwise, the previous
   * byte is finalized, and bytes containing all zeros are added till the byte (8 locations)
   * that contain the index of the incoming Kmer is reached. Then, the index of the byte that
   * contains the kmer is set to indicate its presence.
   */
  public static class BuildBitVectorReducer extends AvroReducer<CharSequence, Long, Pair<CharSequence,Long> > {
    private int bitVectorCharacter;
    private Pair<CharSequence,Long> outputPair;
    private final int BYTE_LEN = 8;
    private final int BYTE_MAX_INDEX = 7;
    private Alphabet dnaAlphabet;
    private long byteIndex = 0;
    private long offset = 0;
    private int correctionK;
    private FSDataOutputStream out;
    private Path bitVectorPath;

    public void configure(JobConf job){
      bitVectorCharacter = 0;
      BuildBitVector stage = new BuildBitVector();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      correctionK = (Integer)(definitions.get("K").parseJobConf(job));
      String outPath = (String)(definitions.get("outputpath").parseJobConf(job));
      outputPair = new Pair<CharSequence,Long>("", 0);
      dnaAlphabet = DNAAlphabetFactory.create();

      // Open a file to write the bitVector to.
      bitVectorPath = new Path(FilenameUtils.concat(
          outPath, VECTOR_FILENAME));
      try {
        Configuration conf = new Configuration();
        FileSystem fs = bitVectorPath.getFileSystem(conf);
        if(fs.exists(bitVectorPath)){
          fs.delete(bitVectorPath, true);
        }
        out = fs.create(bitVectorPath, true);
      }
      catch(Exception e){
        sLogger.fatal(
            "A problem occurred trying to create the file:" +
            bitVectorPath.toString(), e);
        System.exit(-1);
      }
    }

    // incoming key,value pairs - (kmer, frequency)
    public void reduce(CharSequence kmer, Iterable<Long> count,
        AvroCollector<Pair<CharSequence,Long>> output, Reporter reporter) throws IOException {
      outputPair.set(kmer, count.iterator().next());
      output.collect(outputPair);
      long kmerIndex = getKmerIndex(kmer);
      addRelevantCharactersToBitvector(kmerIndex);
    }

    /**
     * Writes the integer to the bitVector.
     * @param value
     */
    private void writeToVector(int value) {
      try {
        out.writeByte(bitVectorCharacter);
      } catch (IOException e) {
        sLogger.fatal(
            "There was a problem writing the value: " + bitVectorCharacter +
            " to " + bitVectorPath.toString(), e);
        System.exit(-1);
      }
    }

    /**
     * This method adds relevant characters to the bitvector till the kmerIndex.
     * kmerIndex is a trusted KMer so we want to set the bit corresponding
     * to KMerIndex to 1 and all bits up to it to zero
     *
     * This method keeps track of the index of the last kmer added to the bitvector and adds
     * all the characters that are relevant till we reach kmerIndex, which is the new kmer
     * supposed to be added.
     *
     * The assumption is that the kmer indexes given to this method are in sorted order
     * @param kmerIndex - the index of the new kmer to be added
     *
     */
    private void addRelevantCharactersToBitvector(long kmerIndex){
      // The new Kmer is outside the current byte that we have to write
      if(kmerIndex >= (byteIndex+1)*BYTE_LEN){
        // Write out the byte
        writeToVector(bitVectorCharacter);
        //Since we have written out the byte to the bitvector, we need to set it to zero
        bitVectorCharacter = 0;
        // We have written out a full byte, so increment the byteIndex
        byteIndex++;
      }
      // While the kmer index is not contained in byteIndex+1
      // We generate zero bytes and stuff them into the bithash
      while (kmerIndex >= (byteIndex+1)*BYTE_LEN) {
        bitVectorCharacter = 0;
        writeToVector(bitVectorCharacter);
        byteIndex++;
      }

      // At this point everything that does not belong to the byte index
      // that contains kmer has been added to the bitvector. Now we
      // set the kmer belonging to the kmerIndex. We calculate
      // the amount of shift by subtracting offset from BYTE_MAX_INDEX
      offset = kmerIndex - (byteIndex * BYTE_LEN) ;
      long shift = BYTE_MAX_INDEX - offset;
      bitVectorCharacter = bitVectorCharacter | (1<<shift);
    }

    /**
     * Given the kmer, this method calculates its position in the bitvector.
     * This is similar to the method that quake uses to calculate the index of
     * a kmer.
     */
    public long getKmerIndex(CharSequence kmer){
      String kmerString = kmer.toString();
      long kmerIndex = 0;
      for(int i = 0 ; i <kmer.length(); i ++){
        kmerIndex <<= dnaAlphabet.bitsPerLetter();
        kmerIndex |=dnaAlphabet.letterToInt(kmerString.charAt(i));
      }
      return kmerIndex;
    }

    public void close(){
      long maxIndex = (long)Math.pow(dnaAlphabet.size(), correctionK) - 1;
      long previousKmerIndex = byteIndex * BYTE_LEN + offset;
      if(previousKmerIndex < maxIndex){
        // We call this method on a kmer at an index that was not present.
        // We need to remove this set bit
        addRelevantCharactersToBitvector(maxIndex);
        // removing the last bit set, because it's kmer was not present
        bitVectorCharacter = bitVectorCharacter ^ 1;
        writeToVector(bitVectorCharacter);
      }
      // The last Kmer is present
      else if(previousKmerIndex == maxIndex){
        writeToVector(bitVectorCharacter);
      }

      try{
        out.close();
      } catch(Exception e){
        sLogger.fatal(
            "A problem occured trying to close the file:" +
            bitVectorPath.toString(), e);
        System.exit(-1);
      }
    }
  }

  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition cutoff = new ParameterDefinition(
        "cutoff", "The cutoff value which is obtained by running quake on the " +
        "Kmer count part file", Integer.class, 0);

    for (ParameterDefinition def: new ParameterDefinition[] {cutoff}) {
      defs.put(def.getName(), def);
    }
    for (ParameterDefinition def: ContrailParameters.getInputOutputPathOptions()) {
       defs.put(def.getName(), def);
    }

    ParameterDefinition kDef = ContrailParameters.getK();
    defs.put(kDef.getName(), kDef);
    return Collections.unmodifiableMap(defs);
  }

  /**
   * Return the path where the bitvector was written to.
   * @return
   */
  public Path getBitVectorPath() {
    return new Path(FilenameUtils.concat(
        (String) stage_options.get("outputpath"), VECTOR_FILENAME));
  }

  @Override
  public List<InvalidParameter> validateParameters() {
    List<InvalidParameter> items = super.validateParameters();
    Integer correctionK = (Integer)stage_options.get("K");
    // TODO(jeremy@lewi.us): Why is 19 the maximum value? Is it just
    // because quake will run out of memory trying to load the bit vector
    // into memory.
    if(correctionK.intValue() > 19 ||
       correctionK.intValue() <= 0){
      InvalidParameter item = new InvalidParameter(
          "K", "K must be: 0<K<=19");
      items.add(item);
    }
    return items;
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();
    String inputPath = (String) stage_options.get("inputpath");
    FileInputFormat.addInputPath(conf, new Path(inputPath));

    Path outputPath = new Path((String) stage_options.get("outputpath"));
    // Write the output of the mapreduce to a subdirectory of the outputpath.
    String mrOutputPath = FilenameUtils.concat(
        outputPath.toString(), "mroutput");
    FileOutputFormat.setOutputPath(conf, new Path(mrOutputPath));
    AvroJob.setInputSchema(conf, new Pair<CharSequence,Long>("", 0L).getSchema());
    AvroJob.setMapOutputSchema(conf, new Pair<CharSequence,Long>("", 0L).getSchema());
    AvroJob.setOutputSchema(conf, new Pair<CharSequence,Long>("", 0L).getSchema());
    AvroJob.setMapperClass(conf, FilterMapper.class);
    AvroJob.setReducerClass(conf, BuildBitVectorReducer.class);

    // A single reducer is necessary since we want all the kmers to
    // arrive at the single reducer in sorted order. We need this to
    // be able to construct the bitvector serially within the reducer
    conf.setNumReduceTasks(1);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new BuildBitVector(), args);
    System.exit(res);
  }
}
