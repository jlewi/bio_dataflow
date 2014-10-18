package contrail.dataflow;

import java.util.List;

import com.google.cloud.dataflow.sdk.runners.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;

// Test for GcsPath NPE
public class GcsPathTest {
  public static void main(String[] args) throws Exception {
    PipelineOptions options = new PipelineOptions();
    GcsUtil gcsUtil = GcsUtil.create(options);
    String bucket ="data-playground-demo-jlewi";
    String object ="nopath/*";
    GcsPath path = GcsPath.fromComponents(bucket, object);
    List<GcsPath> contents = gcsUtil.expand(path);
    for (GcsPath p : contents) {
      System.out.println(p.toString());
    }
    System.out.println("Done");
  }
}
