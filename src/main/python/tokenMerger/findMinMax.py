#!/usr/bin/python

import numpy as np
import codecs
import json
import sys

dateMap = {}

mergedFile = sys.argv[1]

scoreList = []

with codecs.open(mergedFile, "r", "utf8") as inFile:

	for line in inFile:
		jsonInput = json.loads(line)
		pairs = jsonInput["pairs"]

		if ( len(pairs) == 0 ):
			continue

		scoreList.extend(pairs.values())

scoreArr = np.array(scoreList)

print "Min:", scoreArr.min()
print "Median:", np.median(scoreArr)
print "Mean:", scoreArr.mean()
print "Max:", scoreArr.max()

