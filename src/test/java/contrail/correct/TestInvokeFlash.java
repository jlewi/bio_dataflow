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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import contrail.sequences.MatePair;
import contrail.sequences.FastQRecord;
import contrail.util.FileHelper;

/**
 * This test needs to be manually run because the user needs to supply location of
 * the flash binary. Running this manually is better than hard coding the path
 * of the flash binary which might not work
 */
public class TestFlashExtension {

  //5 records - added a \ at 27:212, 22:239 & 27:226 because of presence of "
  private String joinedFastq = 
      "@SRR022868.8762453/1 TAATTTAACTTTGTCTGATAATTGTACGCTTAAATCAACGTCTTTCGGTAAAATATCAGATTGTGCTGGAATTAATAACACATCATCAACCGTTAATGATT II0IIII8IIIIIIIII=I,IIIIIIIIIII,1+II4)HIBIIII?IIE6*(1I1;I,E&I+I9I5/IA&&3=+#I)%+(5*2&&/0%+#\"/$(%$*0%(+ " + 
      "@SRR022868.8762453/2 AGGCTGTTTTTAATGTGGGAAAGTAAATTTGCAAAAGAATCATTAACGTTTGATGATGTGTTATTAATTCCAGCACAATCTGATATTTTACCGAAAGACGT CIIIIIIIIIII>IIIIII&,5II+(*IIIID:*&5I4.II*II.)CI5AI;5IC+0I%1(6&26&-,.)+#2.*3)%-.0-$'%&'&)%)&,$##&#+'+ " + 
      "@SRR022868.8762473/1 TTTTTATTTAAATTAATCATATAATTGCGAGGAGAATATTATGGATTTCGTTAATAATGATACAAGACAAATTGCTAAAAACTTATTAGGTGTCAAAGTGA IIIIIIII=@=FIIIIIIIIIIIIIIIII3II%I>/I,II=III2IIIIII:<1F/7I<I5B<4-I*9;&-A)I?@,$*)')/020+(;/#@+,,#%6%?' " + 
      "@SRR022868.8762473/2 TTCCACGATGTAGCCTGTATACGTTTGAGTGGTATCCTGATAAATCACTTTGACACCTAATAAGTTTTTAGCAATTTGTCTTGTATCATTATTAACGAAAT IIIIIIIIIIIIIIIIGIIII4I5GII.I)II1&9F/II&:*I&I62(744I723.3=>/90&@5?1,<&1-,'.,/,&-2*+.&&+%/)(--$#(,%##* " + 
      "@SRR022868.8762487/1 AGTAATATTGTTGCGTCTGGTATTGTATTAAGTGTAGTTGTTATTTTTGTCAAAGATCAATCAGATTTATCATTGTATGTATTTACTATTGCTATTGTGAC 8II05IIIIIIIIIIIIIIIFEIII82II/+III?+I?IIFI(IIIII22I6+*I+=0*%F/&7&C:?,?-*9@=3&6@1&<.1%*@%/32&,#./-('#% " + 
      "@SRR022868.8762487/2 ACGAACGAAACAATTGCCAGACGTGTATCCAATTAACCGAAACAAAGCTAATGTATCGTTTTAAATAGATAAACAAAGGTAATTGGTTTAATACCGTCACA 59IABIII6EIIAIIIII;I;EI9I:;IID-&II+9C9I%*(9+-&..;&()5'&6-:'0,/*&*((*%+%#%&&#$*&$&\")%&*%&&#%(''\")$*#&# " + 
      "@SRR022868.8762490/1 TTTATATGAAGAAATGAACCATTTATTGTATTGTAGTTCAGCTGGTCATGAGCCTGGATATATTTATCGCGCTGAAAAAGAAGACTTTGAAGAAATTTCAG III4IHIII=IIIIIIIIIIHIII>IEIIIIIIH9IDII1IIIIB:A.II$I-:CHF(8*.+834(1/@1<4+.''%#'/%%2'#+)*.#%2#%&(')(&) " + 
      "@SRR022868.8762490/2 ATTAAATCATCAAGGTATATTGGAATGTCTTGTTGTTGATATCGTGTTTGTGAACTGATTCCTAACACTCTACCTCTAACTGAAATTTCTTCACATTCTTC ,IIDI:IIIII8III+/I4IIIF,/I$IFHI97=IC6>)8&4.I6?47-1&2%(*2;&+,)(,&&*'&1-)#+%%+)$%))'$$$'%&)%'(%$%+%&$(& " + 
      "@SRR022868.8762492/1 CCAGGATCAAACTCTCCATAAAAATTATGATGTTTGATTAGCTCATAAGTACTAAATAATGTTTGTAACTTATAGTTACGTTTTTTGGAATTAACGTTGAC II;IIBIII8>IIIIII7IDIA0IIIDII6IIIIII,II9IIII4I+3I80II+'.I*1II3<>8;)+4@<+I*F.A,0F.0443*&)&#$<#(-*%+,#' " + 
      "@SRR022868.8762492/2 TGTCGGTAAGAAAAATGAACATTGAAAACTGAATGACAATATGTCAACGTTAATTCCAAAAAACGTAACTATAAGTTACAAACATTATTTAGTACTTATTT IIIIII0IHI?III49B&:852ID'4A;)56.%/B(7(%/)E.*@(+/+#0&)*/(,&&%+(,('$)((+%%$&$%+$(#&$$#$)$&*&&#$#'%&$&!# " ;
  
  // Path of flash binary
  private String flashBinaryPath = "";  
  
  //remember to add spaces after the records - they are delimitors
  private String expectedOutput = 
      "@SRR022868.8762453 TAATTTAACTTTGTCTGATAATTGTACGCTTAAATCAACGTCTTTCGGTAAAATATCAGATTGTGCTGGAATTAATAACACATCATCAAACGTTAATGATTCTTTTGCAAATTTACTTTCCCACATTAAAAACAGCCT II0IIII8IIIIIIIII=I,IIIIIIIIIII,1+II4+HIBIIII?IIE6*(1I1;I0E-I+I9I5/IA.,3=62I6(1(I02CI5;IA#IC).II*II.4I5&*:DIIII*(+II5,&IIIIII>IIIIIIIIIIIC " +
      "@SRR022868.8762473 TTTTTATTTAAATTAATCATATAATTGCGAGGAGAATATTATGGATTTCGTTAATAATGATACAAGACAAATTGCTAAAAACTTATTAGGTGTCAAAGTGATTTATCAGGATACCACTCAAACGTATACAGGCTACATCGTGGAA IIIIIIII=@=FIIIIIIIIIIIIIIIII3II%I>/I,II=III2IIIIII:<1F/7I<I5B<42I*9;,.A,I?@<,1?5@/090>=;/3@7I44762?I&I*:&II/F9&1II)I.IIG5I4IIIIGIIIIIIIIIIIIIIII " + 
      "@SRR022868.8762490 TTTATATGAAGAAATGAACCATTTATTGTATTGTAGTTCAGCTGGTCATGAGCCTGGATATATTTATCGCGCTGAAAAAGAAGAATTTGAAGAAATTTCAGTTAGAGGTAGAGTGTTAGGAATCAGTTCACAAACACGATATCAACAACAAGACATTCCAATATACCTTGATGATTTAAT III4IHIII=IIIIIIIIIIHIII>IEIIIIIIH9IDII1IIIIB:A.II$I-:CHF(8*.+834(1/@1<4+.''%#'/(%2'#+#*.'%2&%'(')())%$)+%%+#)-1&'*&&,(),+&;2*(%2&1-74?6I.4&8)>6CI=79IHFI$I/,FIII4I/+III8IIIII:IDII, " +
      "@SRR022868.8762492 CCAGGATCAAACTCTCCATAAAAATTATGATGTTTGATTAGCTCATAAGTACTAAATAATGTTTGTAACTTATAGTTACGTTTTTTGGAATTAACGTTGACATATTGTCATTCAGTTTTCAATGTTCATTTTTCTTACCGACA II;IIBIII8>IIIIII7IDIA0IIIDII6IIIIII,II9II#!4I+3I80II+*.I*1II3<>8;++4@<+I+F.A,0F.0443*,)/*)<0(-/++@*.E)/%(7(B/%.65);A4'DI258:&B94III?IHI0IIIIII ";
  
  private int blockSize = 3;
  
  public void testMap() throws IOException {
    File temp = FileHelper.createLocalTempDir();
    File outputFile = new File(temp, "output");
    File flashInput = new File(temp, "flashInput.avro");
    runFlashTest(temp, flashInput, outputFile);
    System.out.println("Hadoop job successfully completed");
    File outputAvroFile = new File(outputFile, "part-00000.avro");
    Schema schema = (new FastQRecord()).getSchema();
    DatumReader<FastQRecord> datum_reader =
        new SpecificDatumReader<FastQRecord>(schema);
    DataFileReader<FastQRecord> reader =
        new DataFileReader<FastQRecord>(outputAvroFile, datum_reader);
    int numberOfFastqReads = 0;
    ArrayList<FastQRecord> output = new ArrayList<FastQRecord>();
    while(reader.hasNext()){
      FastQRecord record = reader.next();
      output.add(record);
      numberOfFastqReads++;
    }
    HashMap<String, String> expectedHashMap = getExpectedHashMap();
    assertEquals(expectedHashMap.size(),numberOfFastqReads);
    assertMapOutput(output, expectedHashMap);
    if(temp.exists()){
      FileUtils.deleteDirectory(temp);
    }
    System.out.println("Correctness Test PASSED");

  }
  
  private void runFlashTest(File tempDirectory, File flashInput, File outputPath){
    writeDataToFile(flashInput);
    runApp(flashInput, outputPath);
  }
  
  private void runApp(File input, File output){
    InvokeFlash flashInvoker = new InvokeFlash();
    String[] args =
      {"--inputpath=" + input.toURI().toString(),
       "--outputpath=" + output.toURI().toString(),
       "--flash_binary="+ flashBinaryPath,
       "--block_size=" + blockSize};

    try {
      flashInvoker.run(args);
    } catch (Exception exception) {
      fail("Exception occured:" + exception.getMessage());
    }
  }
  
  private void writeDataToFile(File outFile){
 // Write the data to the file.
    Schema schema = (new MatePair()).getSchema();
    DatumWriter<MatePair> datum_writer =
        new SpecificDatumWriter<MatePair>(schema);
    DataFileWriter<MatePair> writer =
        new DataFileWriter<MatePair>(datum_writer);
    StringTokenizer st = new StringTokenizer(joinedFastq, " ");
    try {
      writer.create(schema, outFile);
      while(st.hasMoreTokens()){
        MatePair mateRecord = getMateRecord(st);
        writer.append(mateRecord);
      }
      writer.close();
    } catch (IOException exception) {
      fail("There was a problem writing to an avro file. Exception:" +
          exception.getMessage());
    } 
  }
   
  private void assertMapOutput(ArrayList<FastQRecord> actualOutput, HashMap<String, String> expectedHashMap){
    Iterator<FastQRecord> iterator = actualOutput.iterator();
    while (iterator.hasNext()) {
      FastQRecord flashedRecord = iterator.next();
      String id = flashedRecord.getId().toString();
      String dna = flashedRecord.getRead().toString();
      String qvalue = flashedRecord.getQvalue().toString();
      String receivedValue = dna + " " + qvalue;
      assertEquals(expectedHashMap.get(id), receivedValue);
    }
  }
  
  /**
   * Reads next 6 records separated by spaces and creates a mateRecord from them
   * @param testStringToken
   * @return
   */
  private MatePair getMateRecord(StringTokenizer testStringToken){
    MatePair mateRecord = new MatePair();
    mateRecord.setLeft(new FastQRecord());
    mateRecord.setRight(new FastQRecord());
    mateRecord.getLeft().setId(testStringToken.nextToken());
    mateRecord.getLeft().setRead(testStringToken.nextToken());
    mateRecord.getLeft().setQvalue(testStringToken.nextToken());
    mateRecord.getRight().setId(testStringToken.nextToken());
    mateRecord.getRight().setRead(testStringToken.nextToken());
    mateRecord.getRight().setQvalue(testStringToken.nextToken());
    return mateRecord;
  }

  private HashMap<String, String> getExpectedHashMap(){
    HashMap<String, String> expectedHashMap = new HashMap<String, String>();
    StringTokenizer tokenizer = new StringTokenizer(expectedOutput, " ");
    while(tokenizer.hasMoreTokens()){
      String seqId = tokenizer.nextToken();
      String dna = tokenizer.nextToken();
      String qvalue = tokenizer.nextToken();
      String expectedString = dna +" " + qvalue;
      expectedHashMap.put(seqId, expectedString);
    }
    return expectedHashMap;
  }
  
  /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException {
    if(args.length!=1 || !args[0].contains("--flash_binary=")){
      throw new IOException("Specify --flash_binary parameter only\nArgument Example: --flash_binary=/path/to/flash/binary");
    }
    TestFlashExtension tester = new TestFlashExtension();
    tester.flashBinaryPath = args[0].substring(args[0].indexOf('=')+1);
    if(tester.flashBinaryPath.trim().length() == 0){
      throw new IOException("Specify --flash_binary parameter only\nArgument Example: --flash_binary=/path/to/flash/binary");
    }
    tester.testMap();
  }

}
