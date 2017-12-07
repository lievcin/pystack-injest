import re
from functools import partial
from shared.context import JobContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import xml.etree.ElementTree as ET

class PostlinksToCsvJobContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'postlinks')

def process_row(row, schema):
  parsed_row = ET.fromstring(row.encode('utf-8'))
  if parsed_row.tag == 'row':
    result = ""
    for key in schema:
      try:
        result += ','
        result += parsed_row.attrib[key]
      except KeyError:
        result += ','
    yield result[1:] #since we introduce a comma at the beginning of each line.

def analyze(sc):
    context = PostlinksToCsvJobContext(sc)

    inputFileName = '/data/stackOverflow2017/PostLinks.xml'
    outputFileName = '/user/group-AI/so_postlinks/output_postlinks'
    fileHeaders = [u'<?xml version="1.0" encoding="utf-8"?>', u'<postlinks>', u'</postlinks>']
    attrExtract = [ 'PostId' ]

    links = sc.textFile(inputFileName) \
        .filter(lambda x: x not in fileHeaders) \
        .map(lambda x: (next(process_row(x, attrExtract)), 1)) \
        .reduceByKey(lambda a,b : a + b) \
        .map(lambda x:x[0]+','+str(x[1]))

    links.saveAsTextFile(outputFileName)