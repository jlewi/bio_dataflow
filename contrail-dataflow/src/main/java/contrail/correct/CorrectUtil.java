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
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.mapred.AvroCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;

import contrail.sequences.FastQFileReader;
import contrail.sequences.FastQRecord;
import contrail.sequences.MatePair;
import contrail.sequences.QuakeReadCorrection;
import contrail.sequences.Read;

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
      sLogger.info(String.format("Writing %d records to: %s", records.size(), filePath));
      FileWriter fstream = new FileWriter(filePath, true);
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
    String fastqString = "@" + seqId + "\n" + dna + "\n+\n" + qvalue;
    return fastqString;
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
   * Convert a fastq record returned by Quake to a Read record.
   * @param fastq
   * @param read
   */
  public void convertFastQToRead(FastQRecord fastq, Read read) {
    // Split the string based on whitespace. "\s" matches a whitespace
    // character and the + modifier causes it to be matched one or more
    // times. An extra "\" is needed to escape the "\s".
    // For more info:
    // http://docs.oracle.com/javase/tutorial/essential/regex/
    String[] tokens = fastq.getId().toString().split("\\s+");

    read.setFastq(fastq);
    // First token should be the read.
    fastq.setId(tokens[0]);

    // Reset quake corrections to the defaults.
    read.getQuakeReadCorrection().setCorrected(false);
    read.getQuakeReadCorrection().setTrimLength(0);
    for (int i = 1; i < tokens.length; ++i) {
      String token = tokens[i];
      if (token.toLowerCase().startsWith("correct")) {
        read.getQuakeReadCorrection().setCorrected(true);
      } else if (token.toLowerCase().startsWith("trim=")) {
        String value = token.split("=", 2)[1];
        read.getQuakeReadCorrection().setTrimLength(Integer.parseInt(value));
      }
    }
  }

  /**
   * Emits a fastQ file from local FS onto HDFS.
   * @param fastqFile : The file to be emitted. The file should include the prefix "file://"
   *   if the file is to be read from the local filesystem.
   * @param output : An instance of the outputCollector
   * @throws IOException
   */
  public void emitFastqFileToHDFS(
      String fastqFile, AvroCollector<FastQRecord> collector, Configuration conf) throws IOException {
	if (!fastqFile.startsWith("file:///")) {
	  throw new RuntimeException(String.format(
			  "The path %s doesn't start with the prefix file:/// which means it won't be read from the local " +
			  "filesystem but use the default filesystem.", fastqFile));
	}
    FastQRecord fastq = new FastQRecord();
    FastQFileReader reader = new FastQFileReader(fastqFile, conf);
    while(reader.hasNext()){
      fastq = reader.next();
      collector.collect(fastq);
    }
    reader.close();
  }

  /**
   * Emits a fastQ file from local FS onto HDFS.
   * @param fastqFile : The file to be emitted
   * @param output : An instance of the outputCollector
   * @throws IOException
   */
  public void emitQuakeFastqFileToHDFS(
      String fastqFile, AvroCollector<Read> collector, Configuration conf) throws IOException{
    if (!fastqFile.startsWith("file:///")) {
	  throw new RuntimeException(String.format(
			  "The path %s doesn't start with the prefix file:/// which means it won't be read from the local " +
			  "filesystem but use the default filesystem.", fastqFile));
	}
	FastQRecord fastq = new FastQRecord();
    Read read = new Read();
    read.setQuakeReadCorrection(new QuakeReadCorrection());
    FastQFileReader reader = new FastQFileReader(fastqFile, conf);
    while(reader.hasNext()){
      fastq = reader.next();
      convertFastQToRead(fastq, read);
      collector.collect(read);
    }
    reader.close();
  }
}
