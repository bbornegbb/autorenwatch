#!/usr/bin/env spark-submit
from __future__ import print_function

import pyspark
import json
import glob
import sys
import operator
import iso8601

if len (sys.argv) > 1 and sys.argv[1] == "short":
    FILE_PATTERN="/mnt/nas/bernhard/zeitung/puma2.inet.tu-berlin.de/~oliver/tagesschau/tagesschau-20161231-*/*-*.json"
    DB_FILE="db/tagesschau_short"
    PARTITIONS=8
else:
    FILE_PATTERN="/tmp/tagesschau.de-api-2013-07-08/*/*/*/*.json"
    DB_FILE="db/tagesschau"
    PARTITIONS=50

def extractTitle (j):
    for titlename in ("headline", "shorttitle"):
        try:
            return j[titlename]
        except:
            pass
    return ""

def extractText (j):
    try:
        return "".join ([ f['text'] for f in j['copytext'] ])
    except:
        pass
    if 'shorttext' in j:
        return j['shorttext']
    return ""

def extractCredits (j):
    try:
        return j['credits']
    except:
        return ""

aNone = ""

def _extract_author (s):
    if s is None:
        return aNone
    if "<em>" in s and "</em>" in s:
        return s.rsplit("<em>", 1)[1].split("</em>", 1)[0]
    else:
        return aNone

def extractAuthorShort (j):
    try:
        text = j['shorttext']
        if ". Von " in text:
            text = text.rsplit(". Von ", 1)[1]
            if len(text.split()) > 6:
                return aNone
            else:
                return [1].rstrip(".")
        else:
            return aNone
    except:
        return aNone

def extractAuthorTeaser (j):
    try:
        return _extract_author (j['teasercopytext'])
    except:
        return aNone

def extractContentType (j):
    try:
        return j['contentType']
    except:
        return aNone

def extractAuthorText (j):
    def _get_author_loc (s):
        if ", " in s:
            return a.split(", ", 2)
        else:
            return [ s, "" ]
    try:
        candidate = j['copytext'][1]['text']
        if True: #candidate.strip().endswith("</em>"):
            author_string = _extract_author (candidate)
        else:
            author_string = None
    except:
        author_string = None
    if author_string is None:
        return aNone, aNone, aNone
    type_ = aNone
    if author_string.startswith ("Ein "):
        type_, author_string = author_string.split(" ", 2)[1:]
    if author_string.lower().startswith ("von "):
        author_string = author_string[4:]
    author_strings = author_string.split (" und ")
    author_strings = reduce (operator.add, (a.split ("; ") for a in author_strings ))
    authors_locs = [ _get_author_loc(a) for a in author_strings ]
    authors = " / ".join (a[0] for a in authors_locs)
    locations = " / ".join (a[1] for a in authors_locs)
    return authors, locations, type_

#def extractAuthorsOther (j):
#    def _extractAuthor ():
#        for field in ['shorttext', 'teasercopytext']:
#            try:
#                text = j[field]
#                yield _extract_author(text)
#            except:
#                pass
#        try:
#            yield _extract (extractText(j))
#        except:
#            pass
#        yield ""
#    return list(set(author for author in _extractAuthor() if not author is None))

def extractDate (j):
    try:
        return iso8601.parse_date (j['date'])
    except:
        return None

def extractMetaFromJson (path):
    with open(path) as jf:
        try:
            j = json.load (jf)
        except:
            print ("*"*80)
            print ("Problem with JSON file: ", path)
            print ("*"*80)
            raise
    text = extractText(j)
    author_text, location, type_ = extractAuthorText (j)
    return (path.rsplit("/", 1)[1],
            extractDate(j),
            extractTitle (j),
            author_text,
            location,
            type_,
            extractAuthorShort (j),
            extractAuthorTeaser (j),
            extractContentType (j),
            text if not text is None else "",
            extractCredits(j))


sc = pyspark.SparkContext("local[8]", "Extractdata (spark, fast)")
spark = pyspark.sql.session.SparkSession.builder\
        .appName(sys.argv[0])\
        .getOrCreate()

paths = [ p for p in glob.glob (FILE_PATTERN) ]
data = sc.parallelize(paths, PARTITIONS).map (extractMetaFromJson)

df = spark.createDataFrame (data, "name date title author_text location type author_short author_teaser content_type text credits".split())
df.write.parquet (DB_FILE, mode="overwrite")
