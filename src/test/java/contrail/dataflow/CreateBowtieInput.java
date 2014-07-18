package contrail.dataflow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import contrail.sequences.Alphabet;
import contrail.sequences.AlphabetUtil;
import contrail.sequences.DNAAlphabetFactory;
import contrail.sequences.FastQRecord;
import contrail.sequences.FastaRecord;
import contrail.sequences.Read;
import contrail.util.AvroFileUtil;
import contrail.util.FileHelper;

/**
 * Create some fake data on GCS to test our pipeline for running bowtie.
 */
public class CreateBowtieInput {
  public static void main(String[] args) throws Exception {

    BowtieInput bowtieInput = new BowtieInput();
    bowtieInput.setReference(new ArrayList<Read>());
    bowtieInput.setQuery(new ArrayList<Read>());

    Random generator = new Random();
    Alphabet alphabet = DNAAlphabetFactory.create();
    for (int i = 0; i < 10; ++i) {
      Read read = new Read();
      read.setFasta(new FastaRecord());
      read.getFasta().setId("Ref-" + i);
      read.getFasta().setRead(AlphabetUtil.randomString(generator, 100, alphabet));
      bowtieInput.getReference().add(read);
    }

    for (int i = 0; i < 10; ++i) {
      // Randomly pick a read to align to.
      int readIndex = generator.nextInt(bowtieInput.getReference().size());
      FastaRecord ref = bowtieInput.getReference().get(readIndex).getFasta();
      int length = 10;
      int pos  = generator.nextInt(ref.getRead().length() - length);

      FastQRecord fastq = new FastQRecord();
      fastq.setId("Query-" + i);
      fastq.setRead(ref.getRead().toString().substring(pos, pos + length));
      fastq.setQvalue(AlphabetUtil.randomString(generator, length, alphabet));

      Read read = new Read();
      read.setFasta(new FastaRecord());
      read.getFasta().setId("Read-" + i);
      read.getFasta().setRead(AlphabetUtil.randomString(generator, 100, alphabet));
      bowtieInput.getQuery().add(read);
    }

    String localDir = FileHelper.createLocalTempDir().getPath();
    String fullFile = FilenameUtils.concat(localDir, "bowtieinput.avro");
    AvroFileUtil.writeRecords(
        new Configuration(), new Path(fullFile), Arrays.asList(bowtieInput));

    System.out.println("FullFile:" + fullFile);
  }
}
