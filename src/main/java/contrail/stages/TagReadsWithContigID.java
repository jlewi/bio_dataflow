package contrail.stages;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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

import contrail.CompressedRead;
import contrail.graph.GraphNodeData;
import contrail.graph.R5Tag;

/**
 * Tag reads with the contig aligned to that read.
 *
 * This is the first step in aligning the reads to the contigs.
 * The output of this MR job is key, value pairs nodeID->SequenceRead.
 * Each pair associates the data for a read with the contig it is aligned with.
 */
public class TagReadsWithContigID extends Stage {
  private static final Logger sLogger = Logger.getLogger(
      TagReadsWithContigID.class);

  public static class Mapper extends
      AvroMapper<Object, Pair<CharSequence, TagReadsOutput>> {


    private Pair<CharSequence, TagReadsOutput> outPair;
    private CompressedRead read;
    public void configure(JobConf job) {
      outPair = new Pair<CharSequence, TagReadsOutput>(
          "", new TagReadsOutput());
    }

    /**
     * Mapper for QuickMerge.
     *
     * Input is an avro file containing the nodes for the graph.
     * For each input, we output the GraphNodeData keyed by the mertag so as to
     * group nodes coming from the same read.
     */
    @Override
    public void map(
        Object input,
        AvroCollector<Pair<CharSequence, TagReadsOutput>> output,
        Reporter reporter) throws IOException {

      if (input instanceof CompressedRead) {
        read = (CompressedRead) input;
        outPair.key(read.getId());
        outPair.value().setRead(read);
        outPair.value().setNodeID("");
        output.collect(outPair);
        return;
      }

      GraphNodeData nodeData = (GraphNodeData) input;
      outPair.value().setRead(null);
      for (R5Tag tag: nodeData.getR5Tags()) {
        outPair.key(tag.getTag());
        outPair.value().setNodeID(nodeData.getNodeId());
        output.collect(outPair);
      }
    }
  }

  // TODO(jlewi): To make the reducer more efficient we should do a secondary
  // sort so that the CompressedRead is always the first entry.
  public static class Reducer extends
      AvroReducer<CharSequence, TagReadsOutput,
                  Pair<CharSequence, CompressedRead>> {
    private Pair<CharSequence, CompressedRead> outPair;
    private ArrayList<String> nodeIDs;

    public void configure(JobConf job) {
      outPair =
          new Pair<CharSequence, CompressedRead>("", new CompressedRead());
      nodeIDs = new ArrayList<String>();
    }

    @Override
    public void reduce(CharSequence readID, Iterable<TagReadsOutput> iterable,
        AvroCollector<Pair<CharSequence, CompressedRead>> collector,
        Reporter reporter) throws IOException {
      nodeIDs.clear();
      for (TagReadsOutput input : iterable) {
        if (input.getRead() != null && input.getNodeID().length() >0) {
          throw new RuntimeException(
              "Both the read and the nodeID are non-null. This should never " +
              "happen. Only one of these fields should be set for each of " +
              "record outputted by the mapper.");
        }
        if (input.getNodeID().length() > 0) {
          nodeIDs.add(input.getNodeID().toString());
        }
        if (input.getRead() != null) {
          byte[] inputBuffer = input.getRead().getDna().array();
          byte[] buffer = Arrays.copyOf(inputBuffer, inputBuffer.length);
          outPair.value().setId(input.getRead().getId().toString());
          outPair.value().setDna(ByteBuffer.wrap(buffer));
          outPair.value().setLength(input.getRead().getLength());
          outPair.value().setMatePairId(input.getRead().getMatePairId());
        }
      }
      for (String nodeID : nodeIDs) {
        outPair.key(nodeID);
        collector.collect(outPair);
      }
    }
  }

  protected Map<String, ParameterDefinition>
      createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      if (def.getName().equals("outputpath")) {
        defs.put(def.getName(), def);
      }
    }

    ParameterDefinition graphPath = new ParameterDefinition(
        "graphpath", "The directory containing the graph.",
        String.class,
        null);
    defs.put(graphPath.getName(), graphPath);

    ParameterDefinition readPath = new ParameterDefinition(
        "readpath",
        "The directory containing the reads. These should be AVRO files " +
        "CompressedRead records.",
        String.class,
        null);
    defs.put(readPath.getName(), readPath);

    return defs;
  }


  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"graphpath", "readpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String readPath = (String) stage_options.get("readpath");
    String graphPath = (String) stage_options.get("graphpath");
    String outputPath = (String) stage_options.get("outputpath");

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf == null) {
      conf = new JobConf(QuickMergeAvro.class);
    } else {
      conf = new JobConf(base_conf, QuickMergeAvro.class);
    }
    this.setConf(conf);

    conf.setJobName("TagReadsWithContigID");

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPath(conf, new Path(graphPath));
    FileInputFormat.addInputPath(conf, new Path(readPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    GraphNodeData graphData = new GraphNodeData();
    CompressedRead read = new CompressedRead();
    ArrayList<Schema> schemas = new ArrayList<Schema>();
    schemas.add(graphData.getSchema());
    schemas.add(read.getSchema());
    Schema inputUnion = Schema.createUnion(schemas);

    AvroJob.setInputSchema(conf, inputUnion);

    Pair<CharSequence, TagReadsOutput> outPair =
        new Pair<CharSequence, TagReadsOutput>("", new TagReadsOutput());
    AvroJob.setMapOutputSchema(conf, outPair.getSchema());

    Pair<CharSequence, CompressedRead> redouceOutput = new
        Pair<CharSequence, CompressedRead>("", new CompressedRead());
    AvroJob.setOutputSchema(conf, redouceOutput.getSchema());

    AvroJob.setMapperClass(conf, Mapper.class);
    AvroJob.setReducerClass(conf, Reducer.class);

    if (stage_options.containsKey("writeconfig")) {
      writeJobConfig(conf);
    } else {
      // Delete the output directory if it exists already
      Path out_path = new Path(outputPath);
      if (FileSystem.get(conf).exists(out_path)) {
        // TODO(jlewi): We should only delete an existing directory
        // if explicitly told to do so.
        sLogger.info("Deleting output path: " + out_path.toString() + " " +
            "because it already exists.");
        FileSystem.get(conf).delete(out_path, true);
      }

      long starttime = System.currentTimeMillis();
      RunningJob result = JobClient.runJob(conf);
      long endtime = System.currentTimeMillis();
      float diff = (float) ((endtime - starttime) / 1000.0);

      sLogger.info("Runtime: " + diff + " s");

      return result;
    }
    return null;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new TagReadsWithContigID(), args);
    System.exit(res);
  }
}
