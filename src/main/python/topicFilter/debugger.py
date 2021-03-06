#!/usr/bin/python

import codecs
import json
import re
import sys
import time
from nltk.stem import WordNetLemmatizer
import pandas as pd

if ( len(sys.argv) < 4 ):
    print "Usage: %s <trec_topics.json> <sparkTrecOutput.csv> <output_file.csv>" % (sys.argv[0])
    exit(1)

topicsFilePath = sys.argv[1]
sparkCsvFilePath = sys.argv[2]
outputPath = sys.argv[3]

topicsJsonObj = None

with codecs.open(topicsFilePath, "r", "utf-8") as f:
    topicsJsonObj = json.load(f)

wordToTopicMap = {}
topicTimeMap = {}

for topic in topicsJsonObj:
    topicTitle = topic["title"]
    topicNum = topic["num"]
    tokens = topic["tokens"]

    for token in tokens:
        if ( token not in wordToTopicMap ):
            wordToTopicMap[token] = [(topicNum,topicTitle)]
        else:
            wordToTopicMap[token].append((topicNum,topicTitle))

    topicTimeMap[topicNum] = {}

wnl = WordNetLemmatizer()
specCharRegex = re.compile(r"[^a-zA-Z0-9\\s]")

outputRows = []
with codecs.open(sparkCsvFilePath, "r", "utf-8") as f:

    df = pd.read_csv(sparkCsvFilePath, header=None)
    for (id, row) in df.iterrows():

        topicNums = row[0]
        captureTime = row[1]
        tweetId = row[2]
        tweetText = row[3]

        gmTime = time.gmtime(captureTime)
        timeTuple = (gmTime.tm_year, gmTime.tm_mon, gmTime.tm_mday)
        timeStr = "%d-%d-%d" % (gmTime.tm_year, gmTime.tm_mon, gmTime.tm_mday)

        cleanTokens = specCharRegex.sub(" ", tweetText.lower(), count=0)
        tokens = set([wnl.lemmatize(x) for x in cleanTokens.split(" ")])

        localTopicCountMap = {}
        localTopics = []
        for token in tokens:

            if ( token in wordToTopicMap ):
                localTopics.extend(wordToTopicMap[token])

                for x in wordToTopicMap[token]:
                    thisTopicNum = x[0]
                    if ( thisTopicNum not in localTopicCountMap ):
                        localTopicCountMap[thisTopicNum] = 1
                    else:
                        localTopicCountMap[thisTopicNum] += 1

        for localTopic in localTopics:
            if ( localTopicCountMap[localTopic[0]] < 2 ):
                continue

            if ( timeTuple in topicTimeMap[localTopic[0]] and len(topicTimeMap[localTopic[0]][timeTuple]) > 10 ):
                continue

            if ( timeTuple not in topicTimeMap[localTopic[0]] ):
                topicTimeMap[localTopic[0]][timeTuple] = [tweetId]
            else:
                topicTimeMap[localTopic[0]][timeTuple].append(tweetId)

            item = {"topic":localTopic[0], "title": localTopic[1], "time":captureTime, "date":timeStr, "id":tweetId, "text":tweetText}
            outputRows.append(item)


outputDf = pd.DataFrame(outputRows)

outputDf.to_csv(outputPath, columns=["topic", "title", "time", "date", "id", "text"], index=False)