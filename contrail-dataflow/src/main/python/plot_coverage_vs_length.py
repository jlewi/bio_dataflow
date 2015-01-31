"""
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Make a plot of the coverage of nodes vs. their lengths.
This requires matplotlib and numpy
"""

__author__ = "jeremy@lewi.us (Jeremy Lewi)"

import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader

import gflags
import logging
from matplotlib import pylab
import numpy as np
import os
import sys

FLAGS = gflags.FLAGS

gflags.DEFINE_string(
    "inputpath", None,
    "The path to the avro files containing the graph")
gflags.DEFINE_string(
    "outputpath", None,
    "Where to save an image of the graph")


def main(argv):
  try:
    argv = FLAGS(argv)  # parse flags
  except gflags.FlagsError as e:
    print "%s\\nUsage: %s ARGS\\n%s" % (e, sys.argv[0], FLAGS)
    sys.exit(1)

  if not FLAGS.inputpath:
    logging.fatal("You must specify an inputpath")
    sys.exit(-1)

  if not FLAGS.outputpath:
    logging.fatal("You must specify an outputpath")
    sys.exit(-1)

  files = os.listdir(FLAGS.inputpath)
  avro_files = []
  for f in files:
    if f.endswith("avro"):
      avro_files.append(f)

  if not avro_files:
    logging.fatal("No avro files were found in:%s" % FLAGS.inputpath)
    sys.exit(-1)

  logging.info("Found avro files:%s" % ",".join(avro_files))

  num_nodes = 0
  dt = np.dtype([("coverage", np.float), ("length", np.int)])

  delta_size = 1000
  data = np.empty(delta_size, dtype=dt)
  for f in avro_files:
    path = os.path.join(FLAGS.inputpath, f)
    reader = DataFileReader(open(path, "r"), DatumReader())
    for node in reader:
      num_nodes += 1
      if num_nodes > data.size:
        old_data = data
        data = np.empty(delta_size + data.size, dtype=dt)
        data[0:num_nodes - 1] = old_data[0:num_nodes - 1]

      index = num_nodes - 1
      data[index]["coverage"] = node["coverage"]
      data[index]["length"] = node["sequence"]["length"]

    reader.close()

  data = data[0:num_nodes]

  # Make a 2-d histogram.
  H, xedges, yedges = np.histogram2d(data["length"], data["coverage"])
  hf_hist = pylab.figure()
  extent = [yedges[0], yedges[-1], xedges[-1], xedges[0]]
  ha_hist = hf_hist.add_subplot(1, 1, 1)
  hm = ha_hist.matshow(np.log10(H + 1), extent=extent)
  ha_hist.invert_yaxis()
  ha_hist.set_title("Histogram (log10(counts +1))")
  ha_hist.set_xlabel("Length")
  ha_hist.set_ylabel("Coverage")
  hf_hist.colorbar(hm, ax=ha_hist)
  ha_hist.xaxis.set_ticks_position("bottom")

  hf = pylab.figure()
  ha = hf.add_subplot(1, 1, 1)
  ha.plot(data["length"], data["coverage"], '.')
  ha.set_xscale("log")
  ha.set_yscale("log")
  ha.set_xlabel("Length")
  ha.set_ylabel("Coverage")

  if not os.path.exists(FLAGS.outputpath):
    os.makedirs(FLAGS.outputpath)

  hf.savefig(os.path.join(FLAGS.outputpath, "cov_vs_length_scatter.png"));
  hf_hist.savefig(os.path.join(FLAGS.outputpath, "cov_vs_length_hist.png"));

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO)
  main(sys.argv)
