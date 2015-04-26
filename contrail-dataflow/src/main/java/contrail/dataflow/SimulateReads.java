package contrail.dataflow;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;

/**
 * This Dataflow uses ART(
 * http://www.niehs.nih.gov/research/resources/software/biostatistics/art/)
 * to generate simulated reads from a reference genome.
 *
 */
public class SimulateReads {
  private static final Logger logger =
      LoggerFactory.getLogger(SimulateReads.class.getName());

  /**
   * Options supported by {@link WordCount}.
   * <p>
   * Inherits standard configuration options.
   */
  public static interface Options extends PipelineOptions {
    @Description("Path or glob for the fastq files to generate squence from.")
    @Default.String("gs://contrail_tmp/human/hs_alt_CHM1_1.1_chr1.fa")
    String getInput();
    void setInput(String value);

    @Description("Path to write the outputs to.")
    @Default.String("gs://contrail_tmp/simulated_human")
    String getOutput();
    void setOutput();

    @Description("The path to the docker container containing ART.")
    @Default.String("gcr.io/_b_contrail_docker_registry/art:20150425")
    String getARTContainer();
    void setARTContainer(int value);

    @Description("The address of the Docker daemon.")
    @Default.String("unix:///var/run/docker.sock")
    String getDockerAddress();
    void setDockerAddress();
  }

  /**
   * A DoFn to generate reads using the ART tool.
   */
  public static class CreateReads extends DoFn<GcsPath, GcsPath> {
    private transient GcsUtil gcsUtil;
    private transient DockerClient dockerClient;

    private String dockerImage;
    private String dockerAddress;

    public CreateReads(String dockerImage, String dockerAddress) {
      this.dockerImage = dockerImage;
      this.dockerAddress = dockerAddress;
    }

    @Override
    public void startBundle(DoFn.Context c) {
      gcsUtil = new GcsUtil.GcsUtilFactory().create(c.getPipelineOptions());
      dockerClient = new DefaultDockerClient(dockerImage);
    }

    @Override
    public void processElement(DoFn<GcsPath, GcsPath>.ProcessContext c)
        throws Exception {

      SeekableByteChannel byteChannel = gcsUtil.open(c.element());

      byteChannel.
      // TODO Auto-generated method stub
      ArrayList<String> command = new ArrayList<String>();
      command.add("/art_bin_ChocolateCherryCake/art_illumina");

      DockerProcessBuilder builder = new DockerProcessBuilder(
          command, dockerClient);
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(options);
    GcsPath inputPattern = GcsPath.fromUri(options.getInput());
    List<GcsPath> inputs;
    try {
      inputs = gcsUtil.expand(inputPattern);
    } catch (IOException e1) {
      logger.error(
          "Could not expand pattern; " + inputPattern +
          " Exception:" + e1.getMessage());
      System.exit(-1);
    }

    // TODO(jeremy@lewi.us): We might want to make each path its own
    // PCollection and then flatten them together to increase likelihood
    // Dataflow will process them in parallel.

    PCollection<GcsPath> inputCollections = p.apply(Create.of(inputs));
//    p.apply(TextIO.Read.named("ReadLines").from(options.getInput()))
//     .apply(new CountWords())
//     .apply(TextIO.Write.named("WriteCounts")
//         .to(options.getOutput())
//         .withNumShards(options.getNumShards()));

    p.run();

    // sLogger.info("Done with close");
    GcsOptions gcsOptions = options.as(GcsOptions.class);
    gcsOptions.getExecutorService().shutdown();
    try {
      gcsOptions.getExecutorService().awaitTermination(3, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      logger.error(
          "Thread was interrupted waiting for execution service to shutdown.");
    }
  }
}
