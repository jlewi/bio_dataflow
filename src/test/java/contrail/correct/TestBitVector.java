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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.avro.mapred.Pair;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;
import contrail.ReporterMock;
import contrail.correct.BuildBitVector.BuildBitVectorReducer;
import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.stages.AvroCollectorMock;
import contrail.stages.ParameterDefinition;
import contrail.util.FileHelper;

public class TestBitVector {

  private String mapTestData = "AAA 5 " + 
                               "ACC 2 " +
                               "ATT 1 " +
                               "ATG 10 " +
                               "AGT 6 " +
                               "CTA 13 " +
                               "TTA 9 ";
  
  private String reducerInData = "AAA 5 " +
                                 "TTT 5 " +
                                 "ATG 10 " +
                                 "CAT 10 " +
                                 "AGT 6 " +
                                 "ACT 6 " +
                                 "CTA 13 " +
                                 "TAG 13 " +
                                 "TTA 9 " + 
                                 "TAA 9 ";
  private List<String> testSequences;
  private List<Long> expectedResults; 
  private List<Long> actualResults; 
  private List<String> reconstructedKmerResults; 
  private int cutoff = 4;
  
  private int K = 3;
  /**
   * BitVector representation in integer format. This was obtained by
   * generating a bitvector from quake, reading the file in binary
   * format and converting individual bytes into integers
   */
  byte[] expectedBitVector = {-127, 18, 16, 8 ,0, 0, -96, 9};

  // Random number generator.
  private Random generator;
  Alphabet dnaAlphabet;

  @Before
  public void setUp() {
      // Create a random generator.
      generator = new Random();
      dnaAlphabet = DNAAlphabetFactory.create();
  }
  
  @Test
  public void testIndexes(){
    int numberOfTests = 10;
    int kmerLength = 5;
    testSequences = new ArrayList<String>();
    expectedResults = new ArrayList<Long>();
    actualResults = new ArrayList<Long>();
    BuildBitVectorReducer reducerObj = new BuildBitVectorReducer();
    reducerObj.createDnaAlphabet();
    for(int i = 0 ; i < numberOfTests; i ++){
      testSequences.add(AlphabetUtil.randomString(generator, kmerLength, dnaAlphabet));
    }
    Collections.sort(testSequences);
    for(int i = 0 ; i < numberOfTests; i ++){
      expectedResults.add(getIndex(testSequences.get(i)));
      actualResults.add(reducerObj.getKmerIndex(testSequences.get(i)));
    }
    
    assertIndexOutput(expectedResults, actualResults);
    reconstructedKmerResults = reconstructKmers(expectedResults, kmerLength);
    assertKmertReconstruction(reconstructedKmerResults,testSequences);
  }
  
  private void assertKmertReconstruction(
      List<String> reconstructedKmerList, List<String> testSequencesList) {
    assertEquals(reconstructedKmerList.size(),testSequencesList.size());
    for(int i = 0 ; i < reconstructedKmerList.size() ; i ++) {
      assertEquals(reconstructedKmerList.get(i), testSequencesList.get(i));
    }
  }

  private List<String> reconstructKmers(List<Long> kmerIndices, int kmerLength) {
    List<String> reconstructedKmers = new ArrayList<String>();
    for(int i = 0 ; i < kmerIndices.size(); i++){
      long index = kmerIndices.get(i);
      String kmer = "";
      int length = kmerLength;
      while(length!=0){
        long remainder = index % dnaAlphabet.size();
        index = index/dnaAlphabet.size();
        char kmerChar = dnaAlphabet.intToLetter((int)remainder);
        kmer=kmerChar + kmer;
        length--;
      }
      reconstructedKmers.add(kmer);
    }
    return reconstructedKmers;
  }
  
  private void assertIndexOutput(List<Long> expectedList,
                                 List<Long> actualList){
    //matching the size of the lists
    assertEquals(expectedList.size(), actualList.size());
    
    //match actual values
    for(int i = 0 ; i < actualList.size(); i ++){
      assertEquals(expectedList.get(i), actualList.get(i));
    }
    
    //assert sorted order
    for(int i = 1 ; i < actualList.size(); i ++){
      assertTrue(actualList.get(i-1)<=actualList.get(i));
    }
  }
  
  private long getIndex(String kmer){
    int length = kmer.length();
    long index = 0;
    for(int i = 0 ; i < length; i ++){
      index += Math.pow(dnaAlphabet.size(), (length-i-1))*
                       (dnaAlphabet.letterToInt(kmer.charAt(i)) );
    }
    return index;
  }
  
  /**
   * Tests if the mapper is pruning correctly according to the cutoff
   * and if both the kmer and its RC are emitted
   */
  @Test
  public void testMap() {
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    BuildBitVector.FilterMapper mapper = new BuildBitVector.FilterMapper();
    BuildBitVector stage = new BuildBitVector();
    JobConf job = new JobConf(BuildBitVector.FilterMapper.class);
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("cutoff").addToJobConf(job, cutoff);
    mapper.configure(job);
    StringTokenizer st = new StringTokenizer(mapTestData, " ");
    AvroCollectorMock<Pair<CharSequence, Long>> collector_mock = new AvroCollectorMock<Pair<CharSequence, Long>>();
    while(st.hasMoreTokens()){
      Pair<CharSequence, Long> countPair = new  Pair<CharSequence, Long>("", new Long(0));
      String kmer = st.nextToken();
      Long count = Long.parseLong(st.nextElement().toString());
      countPair.set(kmer, count);
      try {
        mapper.map(
            countPair,
            collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in map: " + exception.getMessage());
      }  
    }
    HashMap<String, Long> expectedHashMap = getExpectedOutput(reducerInData);
    assertOutput(collector_mock, expectedHashMap);
  }
  
  @Test
  public void testReducer() throws Exception {
    BuildBitVectorReducer reducer = new BuildBitVectorReducer(); 
    HashMap<String, Long> reducerInput = getExpectedOutput(reducerInData);
    JobConf job = new JobConf(BuildBitVectorReducer.class);   
    BuildBitVector stage = new BuildBitVector();
    Map<String, ParameterDefinition> definitions =
        stage.getParameterDefinitions();
    definitions.get("K").addToJobConf(job, K);
    String tempOutput = FileHelper.createLocalTempDir().getAbsolutePath();
    definitions.get("outputpath").addToJobConf(job, tempOutput);
    reducer.configure(job);
    ReporterMock reporter_mock = new ReporterMock();
    Reporter reporter = reporter_mock;
    AvroCollectorMock<Pair<CharSequence, Long>> collector_mock =
        new AvroCollectorMock<Pair<CharSequence, Long>>();
    ArrayList<String> sortedKeys=new ArrayList<String>(reducerInput.keySet());
    Collections.sort(sortedKeys);
    Iterator<String> iter = sortedKeys.iterator();
    //Execute reducer
    while(iter.hasNext()){
      String kmer = iter.next();
      Long counts =  reducerInput.get(kmer);
      ArrayList<Long> countList = new ArrayList<Long>();
      countList.add(counts);
      try {
        reducer.reduce(
            kmer, countList, collector_mock, reporter);
      }
      catch (IOException exception){
        fail("IOException occured in reduce: " + exception.getMessage());
      }
    }
    //triggers bithash computation on outputpath
    reducer.close();
    String bithashFile = new File(tempOutput, "bitvector").getAbsolutePath();
    byte[] actualVector = readBinaryFile(bithashFile);
    
    assertBitVectorOutput(actualVector);
    File temp = new File(tempOutput);
    if(temp.exists()){
      FileUtils.deleteDirectory(temp);
    }
  }
  
  /** 
   * Read the given binary file, and return its contents as a byte array. 
   * @throws FileNotFoundException 
   */ 
  byte[] readBinaryFile(String aInputFileName) throws Exception{
    File file = new File(aInputFileName);
    byte[] result = new byte[(int)file.length()];
    FileInputStream inputStream = new FileInputStream(file);
    inputStream.read(result);
    return result;
  }
  
  /**
   * Asserts the correctness of the output
   * @param collector_mock
   * @param expectedHashMap
   */
  private void assertOutput(AvroCollectorMock<Pair<CharSequence, Long>> collector_mock, HashMap<String, Long> expectedHashMap){
    Iterator<Pair<CharSequence, Long>> iterator = collector_mock.data.iterator();
    int numberOfCountRecords = 0;
    while (iterator.hasNext()) {
      Pair<CharSequence, Long> pair = iterator.next();
      String kmer = pair.key().toString();
      Long count = pair.value();
      //The kmer should always be present 
      assertFalse(expectedHashMap.get(kmer)==null);
      assertTrue(expectedHashMap.get(kmer) == count);
      numberOfCountRecords++;
    }
    assertEquals(expectedHashMap.size(),numberOfCountRecords);
  }
  
  private void assertBitVectorOutput(byte actualBitVector[]){
    //checks if the length is the same
    assertEquals(actualBitVector.length, expectedBitVector.length);
    for(int i = 0 ; i < actualBitVector.length ; i ++){
      assertEquals(actualBitVector[i], expectedBitVector[i]);
    }
  }
  
  private HashMap<String, Long> getExpectedOutput(String inputData){
    StringTokenizer st = new StringTokenizer(inputData, " ");
    HashMap<String, Long> expectedHashMap = new HashMap<String, Long>();
    while(st.hasMoreTokens()){
      String kmer = st.nextToken();
      Long count = Long.parseLong(st.nextElement().toString());
      expectedHashMap.put(kmer, count);
    }
    return expectedHashMap;
  }
}
