#!/usr/bin/python
"""Script to submit Dataflow jobs.
"""
import gflags
import logging
import subprocess
import sys

gflags.DEFINE_boolean("use_service", True, "Whether to run on the service or not.")

FLAGS = gflags.FLAGS

def main(argv):
  #global _api
  try:
    argv = FLAGS(argv)  # parse flags
  except gflags.FlagsError as e:
    print "%s\\nUsage: %s ARGS\\n%s" % (e, sys.argv[0], FLAGS)
    sys.exit(1)

    #args = ["mvn", "exec:java", "-Dexec.mainClass='contrail.dataflow.tools.WriteGraphToBigQuery'", "-Dexec.args='--input=gs://contrail/human/contigs_2015_0110/contrail.stages.CompressAndCorrect/temp/step_01/CompressChains/*.avro' --output_table=human.graph_0201", "-P", "v1"]
  jar = "./target/contrail-dataflow-1.0-SNAPSHOT.jar"
  main_class = "contrail.dataflow.tools.WriteGraphToBigQuery"
  input_path="gs://contrail/human/contigs_2015_0110/contrail.stages.CompressAndCorrect/temp/step_01/CompressChains/*.avro"
  output_table="human.graph_0201"
  args = ["java", "-cp", jar, main_class,
          "--inputpath=" +input_path, "--output_table="+output_table]
      
  if FLAGS.use_service:
    args.extend(["--project=biocloudops", "--runner=BlockingDataflowPipelineRunner",
                 "--stagingLocation=gs://contrail-dataflow/staging"])
  print "Running: %s" % " ".join(args)
  subprocess.check_call(args)  
  
if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main(sys.argv)    
  
        


