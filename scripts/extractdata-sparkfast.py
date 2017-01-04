#!/usr/bin/env spark-submit
from __future__ import print_function

import pyspark
import json
import time
import glob


FILE_PATTERN="/mnt/nas/bernhard/zeitung/puma2.inet.tu-berlin.de/~oliver/tagesschau/tagesschau-20161231-*/*-*.json"

def extractTitle (j):
    return j['headline']

def extractText (j):
    return "".join ([ f['text'] for f in j['copytext'] ])

def extractMetaFromJson (path):
    with open(path) as jf:
        j = json.load (jf)
    return (path.rsplit("/", 1)[1], extractTitle (j), extractText(j))


starttime = time.time()

sc = pyspark.SparkContext("local", "Extractdata (spark, fast)")
spark = pyspark.sql.session.SparkSession.builder\
        .master("local")\
        .appName("scratch")\
        .getOrCreate()

sctime = time.time()

paths = glob.glob (FILE_PATTERN)
data = sc.parallelize(paths).map (extractMetaFromJson)

evalchaintime = time.time()

result = data.collect()
endtime = time.time()

print ("*"*80)
print (result)
print ("*"*80)
print ("Total:", endtime - starttime)
print ("Eval:", endtime - sctime)
print ("Collect:", endtime - evalchaintime)
print ("Setup:", sctime - starttime)
