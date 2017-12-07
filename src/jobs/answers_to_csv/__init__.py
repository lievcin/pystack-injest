import re
from functools import partial
from shared.context import JobContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import xml.etree.ElementTree as ET
import calendar
import time

class AnswersToCsvJobContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'answers')

def process_row(row, schema):
  def process_key(key, value):
    if key in ['Body']:
      value = len(value)
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

def is_a_question(row):
  parsed_row = ET.fromstring(row.encode('utf-8'))
  if parsed_row.tag == 'row':
    if parsed_row.attrib['PostTypeId'] == '1':
        is_question = True
    else:
        is_question = False
    return is_question

def analyze(sc):
    context = AnswersToCsvJobContext(sc)

    inputFileName = '/data/stackOverflow2017/Posts.xml'
    outputFileName = '/user/group-AI/so_answers/output_answers'
    fileHeaders = [u'<?xml version="1.0" encoding="utf-8"?>', u'<posts>', u'</posts>']
    attrExtract = [ 'Id', 'Score', 'ViewCount', 'OwnerUserId', 'CommentCount', 'FavoriteCount', 'Body' ]

		answers = sc.textFile(inputFileName) \
		    .filter(lambda x: x not in fileHeaders) \
		    .filter(lambda x: is_a_question(x) == False) \
		    .map(lambda x:next(process_row(x, attrExtract)))

    answers.saveAsTextFile(outputFileName)