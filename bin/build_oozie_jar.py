#!/usr/bin/python
"""
Setup an HDFS directory for oozie.

We do the following to setup the oozie application directory in HDFS.
1. Delete the directory if it already exists.
2. Create the directory by unpacking the contrail job jar so that
its contents are
app_dir/contrail/...
app_dir/lib/...
3. move app_dir/contrail/... to app_dir/lib/
4. We copy the workflow XML file to app_dir/workflow.xml.
5. We generate job configuration files for the pipeline stages and
which are suitable for use with oozie.
6. We copy the job configuration files to app_dir/stage_configs
"""

__author__ = "jeremy@lewi.us (Jeremy Lewi)"

import logging

import gflags
import os
import shutil
import subprocess
import sys
import tempfile

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
  "path", None, "The workflow application path; the directory on HDFS to store the workflow files")
gflags.MarkFlagAsRequired("path")

gflags.DEFINE_string(
  "workflow", "./share/workflows/workflow.xml", "The workflow XML.")

def main(argv):
  try:
    argv = FLAGS(argv)  # parse flags
  except gflags.FlagsError, e:
    print "%s\nUsage: %s ARGS\n%s" % (e, sys.argv[0], FLAGS)
    sys.exit(1)

  # Check if the application path exists.
  code = subprocess.call(["hadoop", "fs", "-test", "-e", FLAGS.path])
  if code == 0:
    answer = raw_input(
      "Directory {0} exists. Do you want to delete it (y/n)?".format(
        FLAGS.path))
    answer = answer.lower()
    if answer != "y":
      logging.error("Directory {0} exists. Can't continue.".format(FLAGS.path))
      return -1

    code = subprocess.call(["hadoop", "fs", "-rmr", FLAGS.path])
    if code:
      logging.error("Could not delete directory :{0}".format(FLAGS.path))

      return -1
  # Get the contrail jar.
  repo_dir = os.path.dirname(os.path.dirname(__file__))
  repo_dir = os.path.abspath(repo_dir)
  jarpath = os.path.join(repo_dir, "target", "contrail-1.0-SNAPSHOT-job.jar")
  if not os.path.exists(jarpath):
    logging.error("File doesn't exist: {0}.\nPossible causes are:"
                  "\n1. You haven't built the code."
                  "\n2. You aren't running this script from the top of the "
                  "repo.".format(jarpath))
    return -1

  # Create a temporary directory to extract the files to.
  tempdir = tempfile.mkdtemp(prefix="tmpOozie")
  logging.info("Use temp directory: %s", tempdir)

  # Copy the workflow.
  workflow_src = os.path.abspath(FLAGS.workflow)
  workflow_dest = os.path.join(tempdir, "workflow.xml")
  shutil.copyfile(workflow_src, workflow_dest)

  start_dir = os.getcwd()
  try:
    os.chdir(tempdir)
    logging.info("Execute: jar -xvf %s", jarpath)
    subprocess.check_call(["jar", "-xvf", jarpath])

    # Move  tempdir/contrail to tempdir/lib/contrail
    src_path = os.path.join(tempdir, "contrail")
    dest_path = os.path.join(tempdir, "lib", "contrail")
    shutil.move(src_path, dest_path)

    # Create job configuration XML files for each stage.
    stages = ["contrail.avro.FastqPreprocessorAvroCompressed",
              "contrail.avro.BuildGraphAvro"]
    #          "contrail.avro.QuickMergeAvro"]

    # TODO(jlewi): We should really read the stage options from a file
    # such as a properties file or XML file. An XML file would probably
    # work best as we want to be able to inherit options and specify
    # different options for different stages.
    stage_options = {}
    stage_options["BuildGraphAvro"] = {"k": 45}
    for stage in stages:
      stage_name = stage.rsplit(".", 1)[-1]
      stage_file = "%s.xml" % os.path.join(tempdir, "stage_configs", stage_name)
      # We need to include the URI file for the output file because we don't
      # know what the default hadoop filesystem is.
      args = ["hadoop", "jar", jarpath, stage, "--foroozie",
              "--writeconfig=file:///%s" % stage_file,
              "--inputpath=/tmp/proxy", "--outputpath=/tmp/out"]
      options = {}
      if stage_name in stage_options:
        options = stage_options[stage_name]
      args.extend(["--{0}={1}".format(key, value) for key, value in options.items()])
      logging.info("Execute %s", " ".join(args))
      subprocess.check_call(args)

      # Check if a ".crc" file was created and if it was delete it because it
      # will cause problems.
      crc_dir = os.path.dirname(stage_file)
      crc_file = os.path.join(
          crc_dir, "." + os.path.basename(stage_file) + ".crc")
      if os.path.exists(crc_file):
        logging.info("Deleting: %s", crc_file)
        os.unlink(crc_file)

    # Copy the files to the workflow application directory.
    args = ["hadoop", "fs", "-copyFromLocal", tempdir, FLAGS.path]
    subprocess.check_call(args)

  finally:
    logging.info("Used temp directory: %s", tempdir)
    os.chdir(start_dir)
    return

  return

  # Copy the workflow.
if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main(sys.argv)
