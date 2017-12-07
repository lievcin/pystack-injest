import re
from functools import partial
from shared.context import JobContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import xml.etree.ElementTree as ET
import calendar
import time

class QuestionsToCsvJobContext(JobContext):
    def _init_accumulators(self, sc):
        self.initalize_counter(sc, 'questions')

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
    context = QuestionsToCsvJobContext(sc)

    inputFileName = '/data/stackOverflow2017/Posts.xml'
    outputFileName = '/user/group-AI/so_questions/output_questions'
    fileHeaders = [u'<?xml version="1.0" encoding="utf-8"?>', u'<posts>', u'</posts>']
    attrExtract = [ 'Id', 'AcceptedAnswerId', 'Score', 'ViewCount', 'OwnerUserId', 'AnswerCount', 'CommentCount', 'FavoriteCount', 'Body' ]

    questions = sc.textFile(inputFileName) \
        .filter(lambda x: x not in fileHeaders) \
        .filter(lambda x: is_a_question(x) == True) \
        .map(lambda x:next(process_row(x, attrExtract)))

    questions.saveAsTextFile(outputFileName)