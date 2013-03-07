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
import httplib2
import pprint

from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.file import Storage

import gflags
import os
import stat
import sys

gflags.DEFINE_string("path", None, "The path to a file or directory to upload")
gflags.DEFINE_string("name", None, "The name to assign the document. Defaults to the filename.")
gflags.DEFINE_string("description", "", "Description for the file.")
gflags.DEFINE_bool("convert", True, "Whether to convert the file.")

gflags.MarkFlagAsRequired("path")

FLAGS = gflags.FLAGS
FLAGS.UseGnuGetOpt()

# Copy your credentials from the APIs Console
CLIENT_ID = '978102606005.apps.googleusercontent.com'
CLIENT_SECRET = 'WA9ScXrkIQw6ArmiwpImnZ4q'

# Check https://developers.google.com/drive/scopes for all available scopes
OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'

# Redirect URI for installed apps
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'


def NewCredentials():
  """Get a new set of credentials."""
  # Run through the OAuth flow and retrieve credentials
  flow = OAuth2WebServerFlow(CLIENT_ID, CLIENT_SECRET, OAUTH_SCOPE, REDIRECT_URI)
  authorize_url = flow.step1_get_authorize_url()
  print 'Go to the following link in your browser: ' + authorize_url
  code = raw_input('Enter verification code: ').strip()
  credentials = flow.step2_exchange(code)
  return credentials


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

  if not FLAGS.path:
    raise Exception("You must specify a file to upload using the --path argument.")

  # Credential store.
  # TODO(jeremy@lewi.us): We should make sure the credentials file is only readable by
  # the user.
  credentials_file = os.path.join(os.path.expanduser("~"), ".upload_to_drive")
  storage = Storage(credentials_file)

  # TODO(jeremy@lewi.us): How can we detect when the credentials have expired and
  # refresh.
  if os.path.exists(credentials_file):
    # Load the credentials from the file
    credentials = storage.get()
  else:
    # get new credentials
    credentials = NewCredentials()
    storage.put(credentials)

  # Make sure the credentials file is only accessible by the user.
  os.chmod(credentials_file, stat.S_IWUSR | stat.S_IRUSR)

  # Create an httplib2.Http object and authorize it with our credentials
  http = httplib2.Http()
  http = credentials.authorize(http)

  drive_service = build('drive', 'v2', http=http)

  files = []

  if os.path.isdir(FLAGS.path):
    for f in os.listdir(FLAGS.path):
      files.append(os.path.join(FLAGS.path, f))
  else:
    files.append(FLAGS.path)

  for file_path in files:
    # Insert a file
    media_body = MediaFileUpload(file_path, mimetype='text/plain', resumable=True)
    mime_type = "text/plain"

    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext == ".html":
      mime_type = "text/html"

    if FLAGS.name:
      name = FLAGS.name
    else:
      name = os.path.basename(file_path)

    body = {
      'title': name,
      'description': FLAGS.description,
      'mimeType': mime_type
    }

    file = drive_service.files().insert(body=body, media_body=media_body, convert=FLAGS.convert).execute()
    pprint.pprint(file)

if __name__ == "__main__":
  main(sys.argv)
