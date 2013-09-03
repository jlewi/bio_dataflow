#!/usr/bin/python
#
# To use this you need to install the google api python client library
#
# easy_install --upgrade google-api-python-client
# easy_install gflags
# or
#
# pip install --upgrade google-api-python-client
# pip install gflags
#
"""
A simple script to list files on HDFS which aren't on GCS.
"""
import auth_util
import gflags
import os
import httplib2
import pprint
import subprocess
import stat
import sys
import tempfile
from urllib2 import urlparse
import urllib

from apiclient.discovery import build

gflags.DEFINE_string("inputpath", None, "The path on hdfs to treat as the source.")
gflags.DEFINE_string("outputpath", None, "The location on gcs")
gflags.DEFINE_list(
  "ls_command", ["hadoop", "fs", "-ls"], 
  "(Optional) a list containing the command and options to use to list the "
  "files on the input path.")
gflags.DEFINE_bool("copy", False, "Copy the missing files to gcs.")
gflags.DEFINE_bool(
  "dryrun", False, 
  "Just display the commands that would be executed for the copy.")
gflags.DEFINE_string("tempdir", None, "The local temporary directory to use")

gflags.MarkFlagAsRequired("inputpath")
gflags.MarkFlagAsRequired("outputpath")

# The basepath for settings files for this script.
# TODO(jlewi): Should we use a single json file? Or maybe a single directory?
_default_base_path =  os.path.join(
  os.path.expanduser("~"), ".google_apis", '.' + os.path.basename(__file__).split('.')[0])

gflags.DEFINE_string(
  "credentials", _default_base_path + '.credentials', 
  'The path to a file storing the credentials. Defaults to a name ' 
  'based on the script.')
gflags.DEFINE_string(
  "secret", 
  os.path.join(os.path.expanduser("~"), ".google_apis", 'secrets.json'), 
  "The path to a file storing the secret.")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

OAUTH_SCOPE = ['https://www.googleapis.com/auth/devstorage.read_write']

# The gcs service.
global gcs_ 

class FileInfo(object):
  path = None
  size = None
  
  def __init__(self, path, size):
    self.path = path
    self.size = size
    
  @property
  def name(self):
    return os.path.basename(self.path)
  
  
class GCFileInfo(object):
  info = None
  _path = None

  def __init__(self, info):
    self.info = info

  @property
  def path(self):
    if not self._path:
      p = self.info['name']
      # Add a leading slash to be consistent with
      # HDFS paths.
      if not p[0] == '/':
        p = '/' + p
      self._path = p

    return self._path
  
  @property
  def size(self):
    return self.info['size']
  
  @property
  def url(self):
    return self.url["url"]
  
def ListHDFS():
  """List the items on HDFS."""
  command = []
  command.extend(FLAGS.ls_command)
  command.append(FLAGS.inputpath)

  print "List HDFS files. Command:" + " ".join(command)   
  proc = subprocess.Popen(command, stdout=subprocess.PIPE)
  stdout, stderr = proc.communicate()
  retcode = proc.poll()
  print "Command completed. Exit code: %d" % retcode

  if retcode != 0:
    raise Exception(
      "Command:%s failed with exit code %d" % " ".join(command), retcode)
  
  
  lines = stdout.splitlines()
  
  files = []
  
  for l in lines:
    fields = l.split()
    if len(fields) != 8:
      continue
        
    path = fields[-1]
    
    if path in ['.', '..']:
      continue
    size = fields[4]
    
    files.append(FileInfo(path, size))
  
  print "Number of HDFS items: %d" % len(files)
  files = dict([(f.path, f) for f in files])
  return files


def ListGCS():
  global gcs_  
  auth_helper = auth_util.OAuthHelper()
  auth_helper.Setup(
    credentials_file=FLAGS.credentials, secrets_file=FLAGS.secret,
    scopes=OAUTH_SCOPE)
  
  http = auth_helper.CreateHttpClient()
  
  gcs_ = build('storage', 'v1beta2', http=http)
  
  # List the contents of the outputpath which should be on gcs.  
  parsed = urlparse.urlparse(FLAGS.outputpath)
  bucket = parsed.netloc
  objects = gcs_.objects()
  prefix = parsed.path[1:]
  delimiter = '/'
  if prefix[0] == delimiter:
    prefix = prefix[1:]
    
  if prefix[-1] == delimiter:
    prefix = prefix[:-1]
    
  response = objects.list(bucket=bucket, prefix=prefix).execute()
  
  if 'items' not in response:
    raise Exception('No files found in:' + FLAGS.outputpath)
  
  gcs_items = {}
  for i in response['items']:
    gitem = GCFileInfo(i)
    gcs_items[gitem.path] = gitem

  return gcs_items


def CopyFiles(missing_items):
  global gcs_
  for i in missing_items:
    # Copy the file to the local filesystem.
    hf = tempfile.NamedTemporaryFile(prefix=i.name, dir=FLAGS.tempdir)
    temp_name = hf.name
    hf.close()
    
    command = ["hadoop", "fs", "-copyToLocal"]
    command.append(i.path)
    command.append(temp_name)
  
    print " ".join(command)
    if not FLAGS.dryrun:
      proc = subprocess.Popen(command, stdout=subprocess.PIPE)
      stdout, stderr = proc.communicate()
      retcode = proc.poll()
      print "Command completed. Exit code: %d" % retcode
      print "Command Output:" + stdout
      if retcode != 0:
        raise Exception(
          "Command:%s failed with exit code %d" % " ".join(command), retcode)    
    
  
    # Now we need to copy the file to gcs.
    # TODO(jeremy@lewi.us): We should probably use the API rather than
    # the command line tool.
    command = ["gsutil", "cp"]
    command.append(temp_name)
    command.append(os.path.join(FLAGS.outputpath, i.name))
  
    print " ".join(command)
    if not FLAGS.dryrun:
      proc = subprocess.Popen(command, stdout=subprocess.PIPE)
      stdout, stderr = proc.communicate()
      retcode = proc.poll()
      print "Command completed. Exit code: %d" % retcode
      print "Command Output:" + stdout
      if retcode != 0:
        raise Exception(
          "Command:%s failed with exit code %d" % " ".join(command), retcode)    
    
    # Delete the temp file
    print "Deleting:" + temp_name
    if not FLAGS.dryrun:
      os.unlink(temp_name)

def main(argv):
  try:
    unparsed = FLAGS(argv)  # parse flags
  except gflags.FlagsError, e:
    usage = """Usage:
{name} {flags}
"""
    print "%s" % e
    print usage.format(name=argv[0], flags=FLAGS)
    sys.exit(1)

  hdfs_items = ListHDFS()   
  gcs_items = ListGCS()
  
  print "GCS Number of items:%d" % len(gcs_items)
  print "HDFS Number of items:%d" % len(hdfs_items)
  
  # Compare the items.
  missing_items = []
  invalid_size = []
  
  for hitem in hdfs_items.values():
    if not hitem.path in gcs_items:
      missing_items.append(hitem)
      continue
    
    gitem = gcs_items[hitem.path]
    if gitem.size != hitem.size:
      invalid_size.append(hitem)
      
  print "The following items on GCS have incorrect size:"
  print "\n".join([h.path for h in invalid_size])
  
  print ""
  print "The following items are not in gcs:"
  print "\n".join([h.path for h in missing_items])
  
  if FLAGS.copy:
    CopyFiles(missing_items)
  
if __name__ == "__main__":
  main(sys.argv)
