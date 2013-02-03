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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import contrail.ReporterMock;
import contrail.correct.KmerCounter.KmerCounterMapper;
import contrail.correct.KmerCounter.KmerCounterReducer;
import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.stages.AvroCollectorMock;
import contrail.stages.ParameterDefinition;
import contrail.util.FileHelper;

public class TestKmerCounter {

  private String fastqRecords= "@1/1 GCCACGAAATTAGC ?????????????? " +
                               "@2/1 TTATTAATCATTAA ?????????????? ";
  private int K = 10;

  //These kmers will be produced with a count of 1 if input is fastqRecords
  private String expectedKmers = "ATTTCGTGGC " +
                                 "AATTTCGTGG " +
                                 "CACGAAATTA " +
                                 "ACGAAATTAG " +
                                 "CGAAATTAGC " +
                                 "TGATTAATAA " +
                                 "ATGATTAATA " +
                                 "AATGATTAAT " +
                                 "TAATGATTAA " +
                                 "TAATCATTAA " ;

  private String reducerInputKmers = "ATTTCGTGGC " +
                                     "ATTTCGTGGC " +
                                     "AATTTCGTGG " +
                                     "AATTTCGTGG " +
                                     "AATTTCGTGG " +
                                     "CACGAAATTA " +
                                     "ACGAAATTAG ";

  /**
   * Tests the mapper.
   */
  @Test
  public void testMap() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    KmerCounter.KmerCounterMapper mapper = new KmerCounter.KmerCounterMapper();
    KmerCounter stage = new KmerCounter();
    JobConf job = new JobConf(KmerCounter.KmerCounterMapper.class);
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("K").addToJobConf(job, K);
    mapper.configure(job);
    StringTokenizer st = new StringTokenizer(fastqRecords, " ");
    AvroCollectorMock<Pair<CharSequence, Long>> collector_mock = new AvroCollectorMock<Pair<CharSequence, Long>>();
    while(st.hasMoreTokens()){
      FastQRecord record = new FastQRecord();
      record.setId(st.nextToken());
      record.setRead(st.nextToken());
      record.setQvalue(st.nextToken());
      try {
        mapper.map(
            record,
            collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
    }
    HashMap<String, Long> expectedHashMap = getExpectedOutput(expectedKmers);
    assertOutput(collector_mock, expectedHashMap);
  }

  /**
   * Tests the mapper.
   */
  @Test
  public void testMapMatePair() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    KmerCounter.KmerCounterMapper mapper = new KmerCounter.KmerCounterMapper();
    KmerCounter stage = new KmerCounter();
    JobConf job = new JobConf(KmerCounter.KmerCounterMapper.class);
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("K").addToJobConf(job, K);
    mapper.configure(job);
    StringTokenizer st = new StringTokenizer(fastqRecords, " ");
    AvroCollectorMock<Pair<CharSequence, Long>> collector_mock = new AvroCollectorMock<Pair<CharSequence, Long>>();
    while(st.hasMoreTokens()){
      MatePair record = new MatePair();
      record.setLeft(new FastQRecord());
      record.setRight(new FastQRecord());
      String seqId = st.nextToken();
      String read = st.nextToken();
      String qval = st.nextToken();
      record.getLeft().setId(seqId+"/1");
      record.getLeft().setRead(read);
      record.getLeft().setQvalue(qval);
      record.getRight().setId(seqId+"/2");
      record.getRight().setRead(read);
      record.getRight().setQvalue(qval);

      try {
        mapper.map(
            record,
            collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
    }
    HashMap<String, Long> expectedHashMap = getExpectedOutputMatePair(expectedKmers);
    assertOutput(collector_mock, expectedHashMap);
  }


  /**
   * Tests the reducer. The input is taken as a string of kmers which are converted into
   * a format compatible with reducer input. This is passes into the reducer and correctness is verified
   * using assertions.
   */
  @Test
  public void testReduce(){
    KmerCounterReducer reducer = new KmerCounterReducer();
    HashMap<String, ArrayList<Long>> reducerInput = getReducerHashMap(reducerInputKmers);
    HashMap<String, Long> reducedExpectedOutput = getExpectedOutput(reducerInputKmers);
    JobConf job = new JobConf(KmerCounterReducer.class);
    reducer.configure(job);
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    AvroCollectorMock<Pair<CharSequence, Long>> collector_mock =
        new AvroCollectorMock<Pair<CharSequence, Long>>();
    Iterator<String> iter = reducerInput.keySet().iterator();
    while(iter.hasNext()){
      String kmer = iter.next();
      ArrayList<Long> counts =  reducerInput.get(kmer);
      try {
        reducer.reduce(
            kmer, counts, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }
    }
    assertOutput(collector_mock,reducedExpectedOutput);
  }

  @Test
  public void testRun() {
    File temp = FileHelper.createLocalTempDir();
    File countsFile = new File(temp, "output");
    File avroFastqRecordInputFile = new File(temp, "fastqrecord.avro");
    writeDataToFile(avroFastqRecordInputFile);
    runApp(avroFastqRecordInputFile, countsFile);
  }

  @Test
  public void testKmerWithNs() {
    // Test the mapper works when the input sequence contains N's
    KmerCounterMapper mapper = new KmerCounterMapper();
    JobConf conf = new JobConf();
    conf.setLong("K", 3);
    mapper.configure(conf);

    FastQRecord fastQRecord = new FastQRecord();
    fastQRecord.setId("input");
    fastQRecord.setRead("ATCGNNNNCTGNNNNANNA");
    fastQRecord.setQvalue("!!!!!!!!!!!!!!!");
    AvroCollectorMock<Pair<CharSequence, Long>> collector =
        new AvroCollectorMock<Pair<CharSequence, Long>>();

    ReporterMock reporter = new ReporterMock();
    try {
      mapper.map(fastQRecord, collector, reporter);
    } catch (IOException e) {
      fail(e.getStackTrace().toString());
    }

    ArrayList<Pair<CharSequence, Long>>expectedList =
        new ArrayList<Pair<CharSequence, Long>>();
    // Expected kmers will be the canonical versions.
    expectedList.add(new Pair<CharSequence, Long>("ATC", 1L));
    expectedList.add(new Pair<CharSequence, Long>("CGA", 1L));
    expectedList.add(new Pair<CharSequence, Long>("CAG", 1L));

    assertArrayEquals(expectedList.toArray(), collector.data.toArray());
  }

  /**
   * Returns an arraylist of 1's where the size of arraylist is the number of times
   * the kmer appers in reducerInput. This is used to mock the recuder input
   * Eg: ATA
   *     AAT
   *     ATG
   *     ATA
   *
   *     will produce
   *     ATA 1 1
   *     AAT 1
   *     ATG 1
   * @param reducerInput
   * @return
   */
  private HashMap<String, ArrayList<Long>> getReducerHashMap(String reducerInput){
    StringTokenizer st = new StringTokenizer(reducerInput, " ");
    HashMap<String, ArrayList<Long>> expectedOutput = new HashMap<String, ArrayList<Long>>();
    while(st.hasMoreTokens()){
      String kmer = st.nextToken();
      ArrayList<Long> initList;
      if(expectedOutput.get(kmer) == null){
        initList = new ArrayList<Long>();
      }
      else{
        initList = expectedOutput.get(kmer);
      }
      initList.add(1L);
      expectedOutput.put(kmer, initList);
    }
    return expectedOutput;
  }

  /**
   * Asserts the correctness of the output
   * @param collector_mock
   * @param expectedHashMap
   */
  private void assertOutput(AvroCollectorMock<Pair<CharSequence, Long>> collector_mock, HashMap<String, Long> expectedHashMap){
    Iterator<Pair<CharSequence, Long>> iterator = collector_mock.data.iterator();
    while (iterator.hasNext()) {
      Pair<CharSequence, Long> pair = iterator.next();
      String kmer = pair.key().toString();
      Long count = pair.value();
      //The kmer should always be present
      assertFalse(expectedHashMap.get(kmer)==null);
      long newcount = expectedHashMap.get(kmer);
      newcount-=count;
      assertTrue(newcount>=0);
      expectedHashMap.put(kmer, newcount);
    }
    //everything should be exactly zero
    Iterator<String> iter = expectedHashMap.keySet().iterator();
    while(iter.hasNext()) {
      String kmer = iter.next().toString();
      Long count = expectedHashMap.get(kmer);
      assertTrue(count==0);
    }
  }

  private HashMap<String, Long> getExpectedOutput(String inputData){
    StringTokenizer st = new StringTokenizer(inputData, " ");
    HashMap<String, Long> expectedHashMap = new HashMap<String, Long>();
    while(st.hasMoreTokens()){
      String kmer = st.nextToken();
      if(expectedHashMap.get(kmer)==null){
        expectedHashMap.put(kmer, 1L);
      }
      else{
        long count = expectedHashMap.get(kmer);
        count ++;
        expectedHashMap.put(kmer, count);
      }
    }
    return expectedHashMap;
  }

  private HashMap<String, Long> getExpectedOutputMatePair(String inputData){
    HashMap<String, Long> expectedHashMap = getExpectedOutput(expectedKmers);
    Iterator<String> iter = expectedHashMap.keySet().iterator();
    while(iter.hasNext()) {
      String kmer = iter.next().toString();
      Long count = expectedHashMap.get(kmer);
      expectedHashMap.put(kmer, 2*count);
    }
    return expectedHashMap;
  }
  /**
   * writes fastqrecord data to outFile in avro format
   * @param outFile
   */
  private void writeDataToFile(File outFile){
    Schema schema = (new FastQRecord()).getSchema();
    DatumWriter<FastQRecord> datum_writer =
        new SpecificDatumWriter<FastQRecord>(schema);
    DataFileWriter<FastQRecord> writer =
        new DataFileWriter<FastQRecord>(datum_writer);
    StringTokenizer st = new StringTokenizer(fastqRecords, " ");
    try {
      writer.create(schema, outFile);
      while(st.hasMoreTokens()){
        FastQRecord record = new FastQRecord();
        record.setId(st.nextToken());
        record.setRead(st.nextToken());
        record.setQvalue(st.nextToken());
        writer.append(record);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing to an avro file. Exception:" +
          exception.getMessage());
    }
  }

  /**
   * Runs the application
   * @param input
   * @param output
   */
  private void runApp(File input, File output){
    KmerCounter kmerCounter = new KmerCounter();
    String[] args =
      {"--inputpath=" + input.toURI().toString(),
       "--outputpath=" + output.toURI().toString(),
       "--K="+ K};

    try {
      kmerCounter.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
}
