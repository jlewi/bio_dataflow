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
package contrail.stages;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

import contrail.graph.GraphN50StatsData;
import contrail.graph.LengthStatsData;
import contrail.io.CollatingAvroFileContentsIterator;
import contrail.util.ContrailLogger;

/**
 * Compute the N50 stats.
 *
 * The input is a set of LengthStatsData in descending order by length.
 * We serially iterate over the data computing for each length the N50 value
 * for contigs of length n or greater.
 *
 * The runtime is O(N) where N is the number of distinct lengths in the graph.
 * This will be min(# of contigs, max length of contigs). In theory this
 * could be in the millions or billions and this won't scale. However, in
 * practice are longest contigs are O(1K-100K) so this should be fine.
 *
 * We output the data as json records so we can import it into bigquery.
 */
public class GraphN50Stats extends NonMRStage {
  private static final ContrailLogger sLogger = ContrailLogger.getLogger(
      GraphN50Stats.class);

  @Override
  protected Map<String, ParameterDefinition> createParameterDefinitions() {
    HashMap<String, ParameterDefinition> defs =
        new HashMap<String, ParameterDefinition>();

    defs.putAll(super.createParameterDefinitions());

    for (ParameterDefinition def:
      ContrailParameters.getInputOutputPathOptions()) {
      defs.put(def.getName(), def);
    }
    return Collections.unmodifiableMap(defs);
  }

  // Order LengthStatsData in descending order based on length.
  private static class DescendingLengthComparator implements
      Comparator<LengthStatsData> {

    @Override
    public int compare(LengthStatsData o1, LengthStatsData o2) {
      // TODO Auto-generated method stub
      return -1 * o1.getLength().compareTo(o2.getLength());
    }
  }

  private static class Accumulator {
    private final LengthStatsData lastStats;
    private final GraphN50StatsData n50Stats;

    public Accumulator() {
      n50Stats = new GraphN50StatsData();
      n50Stats.setNumContigs(0L);
      n50Stats.setLengthSum(0L);

      lastStats = new LengthStatsData();
      lastStats.setCount(0L);
      lastStats.setLength(0L);
    }

    public void add(LengthStatsData data) {
      if (n50Stats.getNumContigs() == 0) {
        n50Stats.setMinLength(data.getLength().intValue());
        n50Stats.setMaxLength(data.getLength().intValue());
      } else {
        n50Stats.setMinLength(
            Math.min(n50Stats.getMinLength(), data.getLength().intValue()));
        n50Stats.setMaxLength(
            Math.max(n50Stats.getMaxLength(), data.getLength().intValue()));
      }
      Long oldNumContigs = n50Stats.getNumContigs();
      Long newNumContigs = n50Stats.getNumContigs() + data.getCount();

      n50Stats.setLengthSum(
          n50Stats.getLengthSum() + data.getCount() * data.getLength());
      n50Stats.setMeanCoverage(
          (n50Stats.getMeanCoverage() * oldNumContigs +
           data.getCoverageMean() * data.getCount()) / newNumContigs);
      n50Stats.setMeanDegree(
          (n50Stats.getMeanDegree() * oldNumContigs +
           data.getDegreeMean() * data.getCount()) / newNumContigs);

      n50Stats.setNumContigs(newNumContigs);
    }

    public Long getCount() {
      return n50Stats.getNumContigs();
    }

    public Long getTotalLength() {
      return n50Stats.getLengthSum();
    }

    public LengthStatsData getLastStats() {
      return lastStats;
    }

    public GraphN50StatsData getN50StatsData() {
      return n50Stats;
    }
  }

  protected void computeStats(
      Iterable<LengthStatsData> lengthsIterable,
      SpecificDatumWriter<GraphN50StatsData> writer, Encoder encoder) {
    // We iterate over the data accumulating various running sums.
    // The front iterator is never behind the back iterator.
    Iterator<LengthStatsData> back = lengthsIterable.iterator();
    Iterator<LengthStatsData> front = lengthsIterable.iterator();

    Accumulator frontAccumulator = new Accumulator();
    Accumulator backAccumulator = new Accumulator();

    while (front.hasNext()) {
      frontAccumulator.add(front.next());

      // Keep advancing the back iterator until we reach the N50 point.
      while (backAccumulator.getTotalLength() * 2 < frontAccumulator.getTotalLength()) {
        backAccumulator.add(back.next());
      }

      GraphN50StatsData stats = frontAccumulator.getN50StatsData();
      stats.setN50Length(backAccumulator.getN50StatsData().getMinLength());

      // Compute the N50 index.

      // Compute the total length of the contigs excluding the most recent
      // length stats added to the backAccumulator.
      Long lastSum =
          backAccumulator.getTotalLength() -
          backAccumulator.getLastStats().getCount() *
          backAccumulator.getLastStats().getLength();

      // Compute how many contigs from the most recent length bucket
      // we need to put us over the N50 limit.
      Long numLastBucket = (long) Math.ceil(
          (frontAccumulator.getTotalLength() / 2.0 - lastSum) /
          backAccumulator.getLastStats().getLength());

      Long n50Index =
          numLastBucket +
          backAccumulator.getCount() -
          backAccumulator.getLastStats().getCount();

      stats.setN50Index(n50Index.intValue());

      try {
        writer.write(stats, encoder);
      } catch (IOException e) {
        sLogger.fatal("Could not write the N50Stats.", e);
      }
    }
  }

  @Override
  protected void stageMain() {
    CollatingAvroFileContentsIterator<LengthStatsData> statsIterator =
        CollatingAvroFileContentsIterator.fromGlob(
            (String) this.stage_options.get("inputpath"), getConf(),
            new DescendingLengthComparator());

    FileSystem fs = null;
    try{
      fs = FileSystem.get(getConf());
    } catch (IOException e) {
      sLogger.fatal("Can't get filesystem: " + e.getMessage(), e);
    }

    String outputFile = (String) this.stage_options.get("outputpath");
    Path outPath = new Path(outputFile);
    try {
      Schema schema = new GraphN50StatsData().getSchema();
      SpecificDatumWriter<GraphN50StatsData> writer =
          new SpecificDatumWriter<GraphN50StatsData>(schema);

      FSDataOutputStream outStream = outPath.getFileSystem(getConf()).create(
          outPath);
      JsonFactory factory = new JsonFactory();
      JsonGenerator generator = factory.createJsonGenerator(outStream);
      MinimalPrettyPrinter printer = new MinimalPrettyPrinter();
      printer.setRootValueSeparator("\n");
      generator.setPrettyPrinter(printer);
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, generator);

      computeStats(statsIterator, writer, encoder);
      encoder.flush();
      outStream.flush();
      outStream.close();
    } catch (IOException e) {
      sLogger.fatal("IOException.", e);
    }
  }

  /**
   * Helper class for reading the output produced by this stage.
   */
  public static class GraphN50StatsFileReader implements
      Iterable<GraphN50StatsData>, Iterator<GraphN50StatsData> {
    private final String path;
    private final Configuration conf;
    private FSDataInputStream inStream;
    private SpecificDatumReader<GraphN50StatsData> reader;
    private GraphN50StatsData stats;
    private JsonDecoder decoder;
    private Boolean hasNext_;

    public GraphN50StatsFileReader(String path, Configuration conf) {
      this.path = path ;
      this.conf = conf;
    }

    private void openFile() {
      FileSystem fs = null;
      try{
        fs = new Path(path).getFileSystem(conf);
      } catch (IOException e) {
        throw new RuntimeException("Can't get filesystem: " + e.getMessage());
      }

      try {
        inStream = fs.open(new Path(path));
        Schema schema = new GraphN50StatsData().getSchema();
        reader = new SpecificDatumReader<GraphN50StatsData>(schema);
        decoder = DecoderFactory.get().jsonDecoder(schema, inStream);
      } catch (IOException exception) {
        throw new RuntimeException(
            "There was a problem reading the file: " + path + " " +
                "Exception:" + exception.getMessage());
      }
    }

    @Override
    public boolean hasNext() {
      if (inStream == null) {
        openFile();
        stats = new GraphN50StatsData();
      }

      if (hasNext_ == null ) {
        try {
          reader.read(stats, decoder);
          hasNext_ = true;
        } catch(EOFException e) {
          // No more data.
          stats = null;
          hasNext_ = false;
        } catch (IOException e) {
          throw new RuntimeException(
              "There was a problem reading the file: " + path + " " +
                  "Exception:" + e.getMessage());
        }
      }

      return hasNext_;
    }

    @Override
    public GraphN50StatsData next() {
      hasNext_ = null;
      return stats;
    }

    @Override
    public void remove() {
      // TODO Auto-generated method stub
      throw new RuntimeException("Not supported.");
    }

    @Override
    public Iterator<GraphN50StatsData> iterator() {
      // TODO Auto-generated method stub
      return new GraphN50StatsFileReader(path, conf);
    }
  }

  public static void main(String[] args) throws Exception {
    GraphN50Stats stage = new GraphN50Stats();
    int res = stage.run(args);
    System.exit(res);
  }
}
