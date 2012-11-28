package contrail.scaffolding;

import java.util.Collection;

import org.apache.commons.lang.StringUtils;

/**
 * This class is used to run the bowtie short read aligner.
 */
public class BowtieRunner {
  private String bowtiePath;
  private String bowtieBuildPath;
  /**
   * Construct the runner.
   * @param bowTiePath: Path to bowtie.
   * @param bowtieBuildPath: Path to bowtie-build the binary for building
   *   bowtie indexes.
   */
  public BowtieRunner(String bowtiePath, String bowtieBuildPath) {
    this.bowtiePath = bowtiePath;
    this.bowtieBuildPath = bowtieBuildPath;
  }

  /**
   * Build the bowtie index by invoking bambus.
   *
   * @param contigFiles
   * @param outBase
   */
  protected void bowtieBuildIndex(
      Collection<String> contigFiles, String outBase) throws Exception {
    String fileList = StringUtils.join(contigFiles, ",");
    String command = String.format("%s %s %s", bowtiePath, fileList, outBase);
    Process p = Runtime.getRuntime().exec(command);
    p.waitFor();
  }
}
