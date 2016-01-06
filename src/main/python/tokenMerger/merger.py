#!/usr/bin/python

import codecs
import json
import sys

dateMap = {}

mergedFile = sys.argv[1]

for tokenFile in sys.argv[2:]:

	with codecs.open(tokenFile, "r", "utf8") as inFile:

		for line in inFile:

			localMap = json.loads(line)
			thisTime = localMap["date"]
			thisMap = localMap["pairs"]

			if ( thisTime not in dateMap ):
				dateMap[thisTime] = thisMap
			else:
				mapToMerge = dateMap[thisTime]

				for token in thisMap.keys():
					if ( token not in mapToMerge ):
						mapToMerge[token] = thisMap[token]

with codecs.open(mergedFile, "w", "utf8") as outFile:

	for (date, pairs) in sorted(dateMap.items()):
		jsonOutput = json.dumps({"date": date, "pairs": pairs})
		outFile.write(jsonOutput + "\n")


