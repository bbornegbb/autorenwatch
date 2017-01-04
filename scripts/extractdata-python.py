#!/usr/bin/env python
from __future__ import print_function

import glob
import json
import time


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

paths = glob.glob (FILE_PATTERN)
result = []

for path in paths:
    result.append (extractMetaFromJson (path))

endtime = time.time()

print ("Total:", endtime - starttime)
