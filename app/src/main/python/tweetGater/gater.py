#!/usr/bin/python

import sys
import re

gatedTweetPath = sys.argv[1]
inputPath = sys.argv[2]
outputPath = sys.argv[3]

tweetIdRegEx = re.compile("[0-9]{18}")

gatedTweetSet = set()
with open(gatedTweetPath, "r") as f:
	for l in f:
		gatedTweetSet.add(long(l))

# print gatedTweetSet

outputFile = open(outputPath, "w")

tweetIdIndex = None

with open(inputPath, "r") as f:
	firstLine = f.next()
	firstLine = firstLine.replace("\t", " ")
	arr = firstLine.split(" ")

	for i, e in enumerate(arr):
		# print i, e
		if ( tweetIdRegEx.match(e) ):
			tweetIdIndex = i

			break

# print tweetIdIndex

with open(inputPath, "r") as f:
	for l in f:
		l = l.replace("\t", " ")
		arr = l.split(" ")
		tweetId = long(arr[tweetIdIndex])

		if ( tweetId in gatedTweetSet ):
			outputFile.write(l)

outputFile.close()
