package contrail.dataflow;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.io.AvroIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;

import contrail.util.FileHelper;
import dataflow.GcsHelper;
import dataflow.docker.DockerProcessBuilder;
import dataflow.docker.HostMountedPath;

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
    void setOutput(String value);

    @Description("The path to the docker container containing ART.")
    @Default.String("gcr.io/_b_contrail_docker_registry/art:20150425")
    String getARTContainer();
    void setARTContainer(String value);

    @Description("The address of the Docker daemon.")
    @Default.String("unix:///var/run/docker.sock")
    String getDockerAddress();
    void setDockerAddress(String value);
  }

  /**
   * Data structure containing information about simulated reads.
   */
  @DefaultCoder(AvroCoder.class)
  public static class SimulatedReadData {
    /**
     * Path to the file containing the actual sequence data.
     */
    public String sequenceFile;

    /**
     * Path to the file containing the reads.
     */
    public String readFile;

    /**
     * Coverage to generate the reads with.
     */
    public int foldCoverage;

    /**
     * Length of the reads to generate.
     */
    public int readLength;
  }


  /**
   * A DoFn to generate reads using the ART tool.
   */
  public static class CreateReads extends
      DoFn<SimulatedReadData, SimulatedReadData> {
    private transient GcsUtil gcsUtil;
    private transient GcsHelper gcsHelper;
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
      dockerClient = new DefaultDockerClient(dockerAddress);
      gcsHelper = new GcsHelper(gcsUtil);

      // TODO(jeremy@lewi.us): We need to pull the image.
    }

    @Override
    public void processElement(
        DoFn<SimulatedReadData, SimulatedReadData>.ProcessContext c)
        throws Exception {
      SimulatedReadData readData = c.element();
      File localTempDir = FileHelper.createLocalTempDir();
      String localInput = FilenameUtils.concat(
          localTempDir.getPath(),
          FilenameUtils.getName(readData.readFile));

      gcsHelper.copyToLocalFile(
          GcsPath.fromUri(readData.sequenceFile), localInput);

      HostMountedPath tempMountedPath = new HostMountedPath(
          localTempDir.getPath(), localTempDir.getPath());

      // TODO Auto-generated method stub
      ArrayList<String> command = new ArrayList<String>();

      HostMountedPath inputMapping = new HostMountedPath(
          localInput, localInput);
      HostMountedPath outputMapping = tempMountedPath.append("output");

      // TODO(jeremy@lewi.us): This is generating single end reads. Should
      // we generate pair end reads.
      command.add("/art_bin_ChocolateCherryCake/art_illumina");
      command.add("-i");
      command.add(inputMapping.getContainerPath());

      // TODO(jeremy@lewi.us): Make fold coverage a parameter.
      command.addAll(Arrays.asList("-f", "10"));

      // TODO(jeremy@lewi.us): Make read length a parameter.
      command.addAll(Arrays.asList("-l", "100"));
      command.addAll(Arrays.asList("-o", outputMapping.getContainerPath()));

      DockerProcessBuilder builder = new DockerProcessBuilder(
          command, dockerClient);
      builder.addVolumeMapping(
          tempMountedPath.getHostPath(),
          tempMountedPath.getContainerPath());
      builder.setImage(this.dockerImage);

      // Start and run the container.
      builder.start();

      // TODO(jeremy@lewi.us): We need to copy the output to GCS and emit
      // the output file name.
      String localOutputFile = outputMapping.getHostPath() + ".fq";
      gcsHelper.copyLocalFileToGcs(
          localOutputFile, GcsPath.fromUri(readData.readFile), "text/plain");
      c.output(readData);
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    GcsUtil gcsUtil = new GcsUtil.GcsUtilFactory().create(options);
    GcsPath inputPattern = GcsPath.fromUri(options.getInput());
    List<GcsPath> inputPaths = null;
    try {
      inputPaths = gcsUtil.expand(inputPattern);
    } catch (IOException e1) {
      logger.error(
          "Could not expand pattern; " + inputPattern +
          " Exception:" + e1.getMessage());
      System.exit(-1);
    }

    List<SimulatedReadData> inputs = new ArrayList<SimulatedReadData>();
    for (GcsPath gcsPath : inputPaths) {
      SimulatedReadData data = new SimulatedReadData();
      data.sequenceFile = gcsPath.toString();
      GcsPath output = GcsPath.fromUri(options.getOutput());
      data.readFile = output.resolve(
          gcsPath.getName(gcsPath.getNameCount() -1 )).toString();
      data.foldCoverage = 10;
      data.readLength = 100;
      inputs.add(data);
    }

    // TODO(jeremy@lewi.us): We might want to make each path its own
    // PCollection and then flatten them together to increase likelihood
    // Dataflow will process them in parallel.

    PCollection<SimulatedReadData> inputCollections = p.apply(
        Create.of(inputs)).setCoder(AvroCoder.of(SimulatedReadData.class));
    inputCollections.apply(ParDo.of(new CreateReads(
        options.getARTContainer(), options.getDockerAddress())))
        .apply(AvroIO.Write.named("WriteOutputInfo")
           .withSchema(SimulatedReadData.class)
           .to(options.getOutput()));

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
