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
// Author: Jeremy Lewi (jeremy@lewi.us)

package contrail.scaffolding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.Sequence;
import contrail.stages.ContrailParameters;
import contrail.stages.ParameterDefinition;
import contrail.stages.Stage;

/**
 * Creates a Tigr file to use as input to bambus.
 *
 * The Tigr file contains information about the contigs in an assembly as
 * well as all the reads aligned to it.
 * The format is described here:
 * TIGR/GDE format:
 * see http://sourceforge.net/apps/mediawiki/amos/index.php?title=Bank2contig
 *
 * see also:
 * http://www.cbcb.umd.edu/research/contig_representation.shtml
 *
 * There are two inputs to the stage
 *   1. The avro file containing GraphNode's representing the contigs.
 *   2. The avro files containing the output of Bowtie. This file describes
 *   how reads align to the contigs.
 *
 * This stage joins the read mappings and contigs using the id of the contigs.
 * For each contig, a record is written to the output file.
 *
 * Important: Since bambus expects a single tigr file we use a single reducer.
 * However, we could make this code more efficient by using multiple reducers
 * and then just copying the output of the files into a single file to
 * pass to bambus.
 *
 * TODO(jeremy@lewi.us): We could also improve the code by using a secondary
 * sort so that the contig always appears first in the reducer. This
 * would make it unnecessary to load the contig and all the mappings into
 * memory for each contig.
 */
public class TigrCreator extends Stage {
  private static final Logger sLogger = Logger.getLogger(TigrCreator.class);
  /**
   * Return the union schema used for the input.
   * @return
   */
  public static Schema inputSchema() {
    ArrayList<Schema> schemas = new ArrayList<Schema>();
    GraphNodeData node = new GraphNodeData();
    BowtieMapping mapping = new BowtieMapping();

    // We need to create a schema representing the union of CompressedRead
    // and FastQRecord because we want to accept either schema for the
    // input.
    schemas.add(node.getSchema());
    schemas.add(mapping.getSchema());
    Schema unionSchema = Schema.createUnion(schemas);

    return unionSchema;
  }

  /**
   * Get the options required by this stage.
   */
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    ParameterDefinition output = new ParameterDefinition(
        "outputpath", "The directory where the output should be written to.",
        String.class, null);

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  public static class TigrMapper extends
    AvroMapper<Object, Pair<CharSequence, Object>> {
    //public QuickMarkMessage msg= null;
    //public GraphNode node= null;

    private Pair<CharSequence, Object> outPair;
    public void configure(JobConf job) {
      outPair = new Pair<CharSequence, Object>("", new BowtieMapping());
    }

    @Override
    public void map(
        Object input, AvroCollector<Pair<CharSequence, Object>> collector,
        Reporter reporter) throws IOException {
      if (input instanceof GraphNodeData) {
        GraphNodeData data = (GraphNodeData) input;

        // TODO(jeremy@lewi.us): It might more sense to just pass along
        // the sequence for the contig and not all the other metadata.
        // However by keeping track of R5Tags we can use the bowtie mappings
        // to validate the R5Tags.
        // Clear the neighbors because we don't need that information.
        data.getNeighbors().clear();
        outPair.key(data.getNodeId());
        outPair.value(data);
        collector.collect(outPair);
        reporter.incrCounter("Contrail", "nodes", 1);
      } else if (input instanceof BowtieMapping) {
        BowtieMapping mapping = (BowtieMapping) input;
        outPair.key(mapping.getContigId());
        outPair.value(mapping);
        collector.collect(outPair);
        reporter.incrCounter("Contrail", "nodes", 1);
      } else {
        sLogger.fatal(
            "Input wasn't a BowtieMapping or GraphNode. Its class was:" +
            input.getClass().getName());
        System.exit(-1);
      }
    }
  }

  protected static class Range {
    final public int start;
    final public int end;
    public Range(int start, int end) {
      this.start = start;
      this.end = end;
    }

    /**
     * Copy constructor.
     */
    public Range(Range other) {
      this(other.start, other.end);
    }

    /**
     * Construct a new range by swapping end and start.
     * @return
     */
    public Range swap() {
      return new Range(this.end, this.start);
    }
  }

  public static class TigrReducer extends MapReduceBase implements
      Reducer<AvroKey<CharSequence>, AvroValue<Object>, Text, NullWritable> {
    GraphNode node = null;
    ArrayList<BowtieMapping> mappings = null;
    Text outLine = null;
    Object value = null;
    Pattern pattern;

    public void configure(JobConf job) {
      node = new GraphNode();
      mappings = new ArrayList<BowtieMapping>();
      outLine = new Text();
      // Match white space one or more times.
      pattern = Pattern.compile("\\s+");
    }

    @Override
    public void reduce(
        AvroKey<CharSequence> key, Iterator<AvroValue<Object>> iterator,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
            throws IOException  {
      mappings.clear();
      int sawnode = 0;
      while(iterator.hasNext()) {
        value = iterator.next().datum();

        if(value instanceof GraphNodeData)   {
          node.setData((GraphNodeData) value);
          node= node.clone();
          sawnode++;
        }
        if (value instanceof BowtieMapping)  {
          BowtieMapping mapping = (BowtieMapping) value;
          mapping = (BowtieMapping) SpecificData.get().deepCopy(
              mapping.getSchema(), mapping);
          mappings.add(mapping);
        }
      }

      if (sawnode != 1) {
        sLogger.fatal(String.format(
            "Key: %s Didn't see exactly 1 instance of GraphNodeData. Number " +
            "of instances:%d", key.datum().toString(), sawnode),
            new RuntimeException("No graph node"));
        System.exit(-1);
      }

      // Make sure the contig id doesn't contain whitespace.
      Matcher matcher = pattern.matcher(node.getNodeId());
      if (matcher.find()) {
        // Trimming the whitespace is probably not a good idea because
        // we would need the id's to be consistent everywhere.
        sLogger.fatal(
            "Contig id contains whitespace: " + node.getNodeId(),
            new RuntimeException("Invalid contig id."));
        System.exit(-1);
      }
      // Output the Tigr record.
      // TODO(jeremy@lewi.us): In the original code
      // http://gage.cbcb.umd.edu/recipes/bambus2/gageBambusPackage.tar.gz
      // The # of bases in the padded consensus was always set to 0. Does
      // this matter?
      int paddedConsensusSize = node.getSequence().size();
      String line = String.format(
          "##%s %d %d bases, 00000000 checksum.", node.getNodeId(),
          mappings.size(), paddedConsensusSize);
      outLine.set(line);
      collector.collect(outLine, NullWritable.get());
      outLine.set(node.getSequence().toString());
      collector.collect(outLine, NullWritable.get());
      // TODO(jeremy@lewi.us): Use the actual read to verify the alignment.
      for (BowtieMapping mapping : mappings) {
        // For the read and the contig we have two sets of indexes.
        // One set of indexes gives the location of the alignment with
        // respect to the "forward" strand of the contig and the read.
        // The forward strand of the contig and the read is whichever
        // strand was provided for the contig and read.
        // A second set of indexes gives the coordinates on the contig
        // and read on the Oriented strand. This will be the same if
        // the forward strand of the read is Oriented to the forward strand
        // of the contig. However, if the read aligns to the reverse complement
        // of the contig then these will be reversed.

        // The mapping record contains the Oriented indexes for the contig.
        // This is zero based.
        Range contigOriented = new Range(
            mapping.getContigStart(), mapping.getContigEnd());

        // If the end index for the contig is smaller then the start index
        // then the read is Oriented to the reverse complement.
        boolean isRC = contigOriented.start > contigOriented.end;

        Range contigForward = contigOriented;
        if (isRC) {
          contigForward = contigOriented.swap();
        }

        if (contigForward.start > contigForward.end) {
          sLogger.fatal(
              "Looks like a bug. contigForward.start should be <= " +
              "contForward.end", new RuntimeException("Alignment bug."));
          System.exit(-1);
        }

        // The coordinates
        // The offset in the contig where the read aligns is the
        // location in the forward strand.
        int contigOffset = contigForward.start;

        String rcStr = "";
        if (isRC) {
          rcStr = "RC";
        }

        // The zero based clear range with respect to the unpadded ungapped
        // sequence. Indexes are inclusive.
        Range readForward = new Range(
            mapping.getReadClearStart(), mapping.getReadClearEnd());
        Range readOriented = readForward;
        if (isRC) {
          readOriented = readForward.swap();
        }

        if (readForward.start > readForward.end) {
          sLogger.fatal(
              "Looks like a bug. readForward.start should be <= " +
              "readForward.endThe start",
              new RuntimeException("Alignment bug."));
          System.exit(-1);
        }

        // Length of the read.  We add 1 because read indexes are inclusive.
        int readLength = readForward.end - readForward.start + 1;

        line = String.format(
            "#%s(%d) [%s] %d bases, 00000000 checksum. {%d %d} <%d %d>",
            mapping.getReadId(), contigOffset, rcStr, readLength,
            readOriented.start + 1 , readOriented.end + 1,
            contigForward.start + 1, contigForward.end +1);

        outLine.set(line);
        collector.collect(outLine, NullWritable.get());

        // If the sequence for the read was included us to verify the alignment.
        // TODO(jeremy@lewi.us): We should make this an option so we don't
        // validate always.
        if (mapping.getRead().length() > 0) {
          // Clear range of the read. We add 1 to end because range is
          // specified inclusively.
          CharSequence clearRead = mapping.getRead().subSequence(
              readForward.start, readForward.end + 1);

          Sequence clearContig = node.getSequence().subSequence(
              contigForward.start, contigForward.end + 1);

          if (clearRead.length() != clearContig.size()) {
            sLogger.fatal(String.format(
                "Contig id: %s Read id%s: The lengths of the clear ranges " +
                "of the read and contig don't match. This probably indicates " +
                "a bug in the code.", node.getNodeId(), mapping.getReadId()),
                new RuntimeException("Alignment bug."));
            System.exit(-1);
          }

          // Check the number of mismatches is what we expected.
          int numMismatches = 0;
          for (int i = 0; i < clearRead.length(); ++i) {
            if (clearRead.charAt(i) != clearContig.at(i)) {
              ++numMismatches;
            }
          }

          if (numMismatches != mapping.getNumMismatches()) {
            sLogger.fatal(String.format(
                "Contig id: %s Read id%s: The alignment doesn't appear to be " +
                "correct. Number of mismatches expected:%d   actual:%d",
                mapping.getContigId(), mapping.getReadId(),
                mapping.getNumMismatches(), numMismatches),
                new RuntimeException("Alignment bug."));
            System.exit(-1);
          }

          // TODO(jeremy@lewi.us): The TIGR format includes the sequence
          // of the actual read but it looks bambus doesn't need that
          // information so we don't include it in the output.
        }
      }
    }
  }

  @Override
  public RunningJob runJob() throws Exception {
    String[] required_args = {"inputpath", "outputpath"};
    checkHasParametersOrDie(required_args);

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    sLogger.info(" - input: "  + inputPath);
    sLogger.info(" - output: " + outputPath);

    Configuration base_conf = getConf();
    JobConf conf = null;
    if (base_conf != null) {
      conf = new JobConf(getConf(), this.getClass());
    } else {
      conf = new JobConf(this.getClass());
    }

    initializeJobConfiguration(conf);

    FileInputFormat.addInputPaths(conf, inputPath);
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroJob.setInputSchema(conf, inputSchema());

    Schema pairSchema = Pair.getPairSchema(
        Schema.create(Schema.Type.STRING), TigrCreator.inputSchema());
    AvroJob.setMapOutputSchema(conf, pairSchema);

    //AvroJob.setOutputSchema(conf, FindBubblesAvro.REDUCE_OUT_SCHEMA);
    AvroJob.setMapperClass(conf, TigrCreator.TigrMapper.class);

    conf.setReducerClass(TigrCreator.TigrReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);
    conf.setOutputFormat(TextOutputFormat.class);

    //delete the output directory if it exists already
    FileSystem.get(conf).delete(new Path(outputPath), true);

    long starttime = System.currentTimeMillis();
    RunningJob result = JobClient.runJob(conf);
    long endtime = System.currentTimeMillis();
    float diff = (float) ((endtime - starttime) / 1000.0);
    sLogger.info("Runtime: " + diff + " s");
    return result;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new TigrCreator(), args);
    System.exit(res);
  }
}
