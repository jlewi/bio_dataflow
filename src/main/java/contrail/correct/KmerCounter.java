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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.DNAUtil;
import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.sequences.Sequence;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * This class counts Kmers. The input is an avro file containing FastQRecord
 * records. Kmers are extracted from the "read" of FastQRecord.
 */

public class KmerCounter extends Stage {
  private static final Logger sLogger = Logger.getLogger(KmerCounter.class);
 /**
  * The input schema to this mapper is the fastqrecord schema
  */

  public static class KmerCounterMapper extends AvroMapper<Object, Pair<CharSequence, Long>> {

    private int K;
    private Sequence dnaSequence;
    private Sequence canonicalSeq;
    private FastQRecord fqRecord = null;
    private MatePair mateRecord = null;
    private CharSequence kmer;
    private CharSequence read;
    /**
     * Configure the mapper
     */
    @Override
    public void configure(JobConf job) {
      KmerCounter stage = new KmerCounter();
      Map<String, ParameterDefinition> definitions = stage.getParameterDefinitions();
      K = (Integer)(definitions.get("K").parseJobConf(job));
      dnaSequence = new Sequence(DNAAlphabetFactory.create());
    }

    /**
     * Mapper emits out <Kmer, 1> pairs
     */
    @Override
    public void map(Object record, AvroCollector<Pair<CharSequence, Long>> collector,
                    Reporter reporter) throws IOException {

      if(record instanceof FastQRecord){
        fqRecord = (FastQRecord)record;
        read = fqRecord.getRead();
        collectKmers(read, collector);
      }

      if(record instanceof MatePair){
        mateRecord = (MatePair)record;
        read = mateRecord.getLeft().getRead();
        collectKmers(read, collector);
        read = mateRecord.getRight().getRead();
        collectKmers(read, collector);
      }
    }

    /**
     * This collects the kmers of length K within the collector
     * @param sequence
     * @param collector
     * @throws IOException
     */
    private void collectKmers(
        CharSequence fullSequence,
        AvroCollector<Pair<CharSequence, Long>> collector) throws IOException {
      // Chop Kmer string into list of strings whenever N character occurs
      // e.g. AATNAANNNGA is chopped into AAT, AA, GA
      String[] pieces = fullSequence.toString().split("N+");
      for (String sequence : pieces) {
        for (int i=0; i<= sequence.length() - K; i++) {
          // We want to treat the Kmer and its reverse complement in the same way
          // So we  find out the canonical kmer and emit that
          kmer = sequence.subSequence(i,(i+K));
          dnaSequence.readCharSequence(kmer);
          canonicalSeq = DNAUtil.canonicalseq(dnaSequence);
          String kmerCanonical = canonicalSeq.toString();
          collector.collect(new Pair<CharSequence,Long>(kmerCanonical, 1L));
        }
      }
    }
  }

  public static class KmerCounterCombiner extends AvroReducer<CharSequence, Long, Pair<CharSequence, Long> > {
    @Override
    public void reduce(CharSequence kmer, Iterable<Long> counts, AvroCollector<Pair<CharSequence,Long>> collector, Reporter reporter) throws IOException {
      KmerCounter kmerObj = new KmerCounter();
      kmerObj.sumAndCollect(kmer, counts, collector, reporter);
    }
   }

  public static class KmerCounterReducer extends AvroReducer<CharSequence, Long, Pair<CharSequence, Long> > {
  @Override
  public void reduce(CharSequence kmer, Iterable<Long> counts, AvroCollector<Pair<CharSequence,Long>> collector, Reporter reporter) throws IOException {
    KmerCounter kmerObj = new KmerCounter();
    kmerObj.sumAndCollect(kmer, counts, collector, reporter);
  }
 }

  public void sumAndCollect(CharSequence kmer, Iterable<Long> counts, AvroCollector<Pair<CharSequence,Long>> collector, Reporter reporter) throws IOException{
    long sum = 0;
    for (long count : counts){
      sum += count;
    }
   collector.collect(new Pair<CharSequence,Long>(kmer, sum));
  }

  @Override
  public RunningJob runJob() throws Exception{
    // Check for missing arguments.
    String[] required_args = {
        "inputpath", "outputpath", "K"};
    checkHasParametersOrDie(required_args);
    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");
    Integer K = (Integer) stage_options.get("K");
    if (K <= 0) {
      sLogger.fatal("K needs to be > 0", new IllegalArgumentException());
      System.exit(-1);
    }

    JobConf conf = new JobConf(KmerCounter.class);
    conf.setJobName("Kmer Counter ");
    initializeJobConfiguration(conf);

    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(new FastQRecord().getSchema());
    schemas.add(new MatePair().getSchema());
    Schema unionSchema = Schema.createUnion(schemas);
    FileInputFormat.setInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));
    AvroJob.setInputSchema(conf, unionSchema);
    AvroJob.setOutputSchema(conf, new Pair<CharSequence,Long>("", 0L).getSchema());
    AvroJob.setMapperClass(conf, KmerCounterMapper.class);
    AvroJob.setCombinerClass(conf, KmerCounterCombiner.class);
    AvroJob.setReducerClass(conf, KmerCounterReducer.class);
    // Delete the output directory if it exists already
    Path out_path = new Path(outputPath);
    if (FileSystem.get(conf).exists(out_path)) {
      FileSystem.get(conf).delete(out_path, true);
    }
    long starttime = System.currentTimeMillis();
    RunningJob run = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) ((endtime - starttime) / 1000.0);
    System.out.println("Runtime: " + diff + " s");
    return run;
  }

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs = new HashMap<String, ParameterDefinition>();
    defs.putAll(super.createParameterDefinitions());
    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new KmerCounter(), args);
    System.exit(res);
  }

}
