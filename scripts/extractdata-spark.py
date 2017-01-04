#!/usr/bin/env spark-submit
from __future__ import print_function

import pyspark
import operator
import time


FILE_PATTERN="/mnt/nas/bernhard/zeitung/puma2.inet.tu-berlin.de/~oliver/tagesschau/tagesschau-20161231-*/*-*.json"

def extractText (row):
    return "".join ( r.text for r in row['copytext'] )

def extractTitle (row):
    return row['headline']

starttime = time.time()
sc = pyspark.SparkContext("local", "Extractdata (spark)")
spark = pyspark.sql.session.SparkSession.builder\
        .master("local")\
        .appName("scratch")\
        .getOrCreate()

sctime = time.time()

jsonl_files = sc.wholeTextFiles(FILE_PATTERN)\
             .map (lambda (name, content): (name, content.replace("\n", "")))

df = spark.read.json(jsonl_files.map (operator.itemgetter(1)))

metadata = df.select('headline', 'copytext').rdd\
        .map (lambda row: (extractTitle(row), extractText(row)))

data = jsonl_files.map (lambda (name, content): name.rsplit("/", 1)[1])\
       .zip(metadata)\
       .map (lambda (x, y): (x,) + y )

evalchaintime = time.time()

result = data.collect()
endtime = time.time()

print ("Total:", endtime - starttime)
print ("Eval:", endtime - sctime)
print ("Collect:", endtime - evalchaintime)
print ("Setup:", sctime - starttime)
