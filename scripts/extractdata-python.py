#!/usr/bin/env python
from __future__ import print_function

import glob
import json
import time
import cPickle as pickle


FILE_PATTERN="/mnt/nas/bernhard/zeitung/puma2.inet.tu-berlin.de/~oliver/tagesschau/tagesschau-20161231-*/*-*.json"
FILE_PATTERN="/tmp/tagesschau.de-api-2013-07-08/*/*/*/*.json"

def extractTitle (j):
    for titlename in ("headline", "shorttitle"):
        try:
            return j[titlename]
        except:
            pass
    return None

def extractText (j):
    try:
        return "".join ([ f['text'] for f in j['copytext'] ])
    except:
        pass
    if 'shorttext' in j:
        return j['shorttext']
    return None

def extractCredits (j):
    try:
        return j['credits']
    except:
        return None

def extractMetaFromJson (path):
    with open(path) as jf:
        j = json.load (jf)
    return (path.rsplit("/", 1)[1], extractTitle (j), extractText(j), extractCredits(j))


starttime = time.time()

paths = glob.glob (FILE_PATTERN)
result = []

for path in paths:
    result.append (extractMetaFromJson (path))

with open ("data.pickle", "w") as f:
    pickle.dump(result, f)

endtime = time.time()

print ("Total:", endtime - starttime)
