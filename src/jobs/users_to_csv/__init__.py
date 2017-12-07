import re
from functools import partial
from shared.context import JobContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import xml.etree.ElementTree as ET
import calendar
import time

class UsersToCsvJobContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'questions')

def process_row(row, schema):
  def process_key(key, value):
    if key in ['WebsiteUrl', 'AboutMe', 'Location']:
      if len(value) > 0:
        value = 1
      else:
        value = 0
    if key in ['CreationDate', 'LastAccessDate']:
      value = calendar.timegm(time.strptime(value, "%Y-%m-%dT%H:%M:%S.%f"))
    return ',' + str(value)
  parsed_row = ET.fromstring(row.encode('utf-8'))
  if parsed_row.tag == 'row':
    result = ""
    for key in schema:
      try:
        result += process_key(key, parsed_row.attrib[key])
      except KeyError:
        result += ',0'
    yield result[1:] #since we introduce a comma for the first key

def analyze(sc):
    context = UsersToCsvJobContext(sc)

    inputFileName = '/data/stackOverflow2017/Users.xml'
    outputFileName = '/user/group-AI/so_users/output_users'
    fileHeaders = [u'<?xml version="1.0" encoding="utf-8"?>', u'<users>', u'</users>']
    attrExtract = [ 'Id', 'Reputation', 'CreationDate', 'LastAccessDate', 'WebsiteUrl', 'Location', 'AboutMe', 'Views', 'UpVotes', 'DownVotes', 'Age' ]

    users = sc.textFile(inputFileName) \
        .filter(lambda x: x not in fileHeaders) \
        .map(lambda x:next(process_row(x, attrExtract)))

    users.saveAsTextFile(outputFileName)