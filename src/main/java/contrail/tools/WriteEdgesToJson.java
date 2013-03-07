/* Licensed under the Apache License, Version 2.0 (the "License");
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
package contrail.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroInputFormat;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import contrail.graph.EdgeDirection;
import contrail.graph.EdgeTerminal;
import contrail.graph.GraphNode;
import contrail.graph.GraphNodeData;
import contrail.sequences.DNAStrand;
import contrail.stages.ContrailParameters;
import contrail.stages.MRStage;
import contrail.stages.ParameterDefinition;

/**
 * Write the edges to json file which can then be imported into BigQuery.
 */
public class WriteEdgesToJson extends MRStage {
  private static final Logger sLogger = Logger.getLogger(WriteEdgesToJson.class);

  /**
   * Get the options required by this stage.
   */
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

  private static class WriteEdgesMapper extends MapReduceBase
      implements Mapper<AvroWrapper<GraphNodeData>, NullWritable,
                        Text, NullWritable> {

    private GraphNode node;
    private Text outKey;

    private ObjectMapper jsonMapper;
    public void configure(JobConf job) {
      node = new GraphNode();
      outKey = new Text();
      jsonMapper = new ObjectMapper();
    }


    private class EdgeInfo {
      public EdgeTerminal source;
      public EdgeTerminal dest;
      public ArrayList<String> tags;

      public EdgeInfo() {
        tags = new ArrayList<String>();
      }
    }

    /**
     * Mapper to do the conversion.
     */
    public void map(AvroWrapper<GraphNodeData> key, NullWritable bytes,
        OutputCollector<Text, NullWritable> collector, Reporter reporter)
            throws IOException {
      node.setData(key.datum());

      EdgeInfo info = new EdgeInfo();

      for (DNAStrand strand : DNAStrand.values()) {
        //info.setSource(node.getNodeId());
        info.source = new EdgeTerminal(node.getNodeId(), strand);
        for (EdgeTerminal terminal : node.getEdgeTerminals(
             strand, EdgeDirection.OUTGOING)) {
          info.dest = terminal;
          info.tags.clear();
          for (CharSequence tag : node.getTagsForEdge(strand, terminal)) {
            info.tags.add(tag.toString());
          }

          Collections.sort(info.tags);
          outKey.set(jsonMapper.writeValueAsString(info));
          collector.collect(outKey, NullWritable.get());
        }
      }
    }
  }

  @Override
  protected void setupConfHook() {
    JobConf conf = (JobConf) getConf();

    String inputPath = (String) stage_options.get("inputpath");
    String outputPath = (String) stage_options.get("outputpath");

    AvroJob.setInputSchema(conf, GraphNodeData.SCHEMA$);

    FileInputFormat.addInputPath(conf, new Path(inputPath));
    FileOutputFormat.setOutputPath(conf, new Path(outputPath));

    AvroInputFormat<GraphNodeData> input_format =
        new AvroInputFormat<GraphNodeData>();
    conf.setInputFormat(input_format.getClass());

    // The output is a text file.
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(NullWritable.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(NullWritable.class);

    // We need to set the comparator because AvroJob.setInputSchema will
    // set it automatically to a comparator for an Avro class which we don't
    // want. We could also change the code to use an AvroMapper.
    conf.setOutputKeyComparatorClass(Text.Comparator.class);
    // We use a single reducer because it is convenient to have all the data
    // in one output file to facilitate uploading to bigquery.
    // TODO(jlewi): Once we have an easy way of uploading multiple files to
    // big query we should get rid of this constraint.
    conf.setNumReduceTasks(1);
    conf.setMapperClass(WriteEdgesMapper.class);
    conf.setReducerClass(IdentityReducer.class);
  }

  private class BigQueryField {
    public String name;
    public String type;
    public String mode;

    public BigQueryField(String name, String type) {
      this.name = name;
      this.type = type;
      fields = new ArrayList<BigQueryField>();
    }

    public BigQueryField() {
      fields = new ArrayList<BigQueryField>();
    }

    public ArrayList<BigQueryField> fields;

    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("{");
      ArrayList<String> pairs = new ArrayList<String>();

      pairs.add(String.format("\"name\":\"%s\"", name));
      pairs.add(String.format("\"type\":\"%s\"", type));

      if (mode != null) {
        pairs.add(String.format("\"mode\":\"%s\"", mode));
      }

      ArrayList<String> subFields = new ArrayList<String>();
      for (BigQueryField subField : fields) {
        subFields.add(subField.toString());
      }

      if (subFields.size() > 0) {
        pairs.add(String.format(
            "\"fields\":[%s]", StringUtils.join(subFields, ",")));
      }

      builder.append(StringUtils.join(pairs, ","));
      builder.append("}");
      return builder.toString();
    }
  }

  protected void postRunHook() {
    // Print out the json schema.
    ArrayList<String> fields = new ArrayList<String>();

    BigQueryField source = new BigQueryField();
    source.name = "source";
    source.type = "record";
    source.fields.add(new BigQueryField("nodeId", "string"));
    source.fields.add(new BigQueryField("strand", "string"));

    BigQueryField dest = new BigQueryField();
    dest.name = "dest";
    dest.type = "record";
    dest.fields.add(new BigQueryField("nodeId", "string"));
    dest.fields.add(new BigQueryField("strand", "string"));


    BigQueryField tags = new BigQueryField();
    tags.name = "tags";
    tags.type = "string";
    tags.mode = "repeated";

    fields.add(source.toString());
    fields.add(dest.toString());
    fields.add(tags.toString());

    String schema = "[" + StringUtils.join(fields, ",") + "]";
    sLogger.info("Schema:\n" + schema);
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(
        new Configuration(), new WriteEdgesToJson(), args);
    System.exit(res);
  }
}
