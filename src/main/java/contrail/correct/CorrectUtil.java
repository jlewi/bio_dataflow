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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.avro.mapred.AvroCollector;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;

/**
 * Utility functions for correction pipeline
 */
public class CorrectUtil {
   private static final Logger sLogger = Logger.getLogger(CorrectUtil.class);
   /**
   * Writes in memory chunk to a file on local cluster nodes
   * @param records: An arraylist of records that belong to the a fastQ file
   * @param filePath: Local file where the in memory fastQ file is written
   */ 
  public void writeLocalFile(ArrayList<String> records, String filePath){
    try{
      FileWriter fstream = new FileWriter(filePath,true);
      BufferedWriter out = new BufferedWriter(fstream);
      for(int i=0;i<records.size();i++){
        out.write(records.get(i)+"\n");
      }
      out.close();
      fstream.close();
    }
    catch (Exception e)	{
      sLogger.error(e.getStackTrace());
    }
 }
  
  /**
   * Adds a given mate pair into in memory arrayLists
   * @param mateRecord : mate paired record
   * @param fastqRecordsMate1 : In memory ArrayList for mate 1
   * @param fastqRecordsMate2 : In memory ArrayList for mate 2
   */ 
  public void addMateToArrayLists(MatePair mateRecord, ArrayList<String> fastqRecordsMateLeft, 
                                  ArrayList<String> fastqRecordsMateRight) {
    fastqRecordsMateLeft.add(fastqRecordToString(mateRecord.getLeft()));
    fastqRecordsMateRight.add(fastqRecordToString(mateRecord.getRight()));
  }
  
  /**
   * converts a fastq record to string
   * @param record
   * @return
   */
  public String fastqRecordToString(FastQRecord record) {
    String seqId = record.getId().toString();
    String dna = record.getRead().toString();
    String qvalue = record.getQvalue().toString();
    String fastqString = seqId+"\n"+dna+"\n"+"+\n"+qvalue;
    return fastqString;
  }

  public void executeCommand(String command){
    sLogger.info("command executed: " + command);
    try {
      Process p = Runtime.getRuntime().exec(command);
      BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line;
      p.waitFor();
      sLogger.info("command Output:");
      while ((line = stdInput.readLine()) != null) {
        sLogger.info(line);
      }	
      sLogger.info("Process Exit Value: " + p.exitValue());
    } 
    catch (Exception e) {
      sLogger.error(e.getStackTrace());
    }
  }
    
  /**
   * Gets the Distributed cache path of a given file named binary
   * @param binary: Name of the file you are looking for
   * @param job: The JobConf object
   * @return
   */
  public String getDcachePath(String binary, JobConf job){
    Path[] dcacheFiles;
    String path= "";
    try {
      dcacheFiles = DistributedCache.getLocalCacheFiles(job);
      if (null != dcacheFiles && dcacheFiles.length > 0) {
        for (Path cachePath : dcacheFiles) {
          if (cachePath.getName().equals(binary)) {
            path = cachePath.toString();
          }      
        }
      } 
    } 
    catch (IOException e) {
      sLogger.error(e.getStackTrace());
    }
    return path;
  }
  
  /**
   * Emits a fastQ file from local FS onto HDFS.
   * @param fastqFile : The file to be emitted
   * @param output : An instance of the outputCollector
   * @throws IOException
   */
  public void emitFastqFileToHDFS(File fastqFile, AvroCollector<FastQRecord> collector) throws IOException{
    FastQRecord read = new FastQRecord();
    FileReader fstream = new FileReader(fastqFile);
    BufferedReader fileReader = new BufferedReader(fstream);
    String outId;
    while((outId = fileReader.readLine())!=null){
      read.setId(outId);
      read.setRead(fileReader.readLine());
      fileReader.readLine();// ignore+
      read.setQvalue(fileReader.readLine());
      collector.collect(read);
    }
    fileReader.close();
    fstream.close();
  }
  
  /**
   * This function writes a string into a file
   * @param data
   * @param filePath
   */
  public void writeStringToFile(String data, String filePath){
    try{
      FileWriter fstream = new FileWriter(filePath,true);
      BufferedWriter out = new BufferedWriter(fstream);
      out.write(data+"\n");
      out.close();
      fstream.close();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
