#!/usr/bin/python

import codecs
import csv
import json
import re
import sys
from nltk.corpus import stopwords
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

for topic in topicsJsonObj:
    topicTitle = topic["title"]
    topicNum = topic["num"]
    tokens = topic["tokens"]

    for token in tokens:
        if ( token not in wordToTopicMap ):
            wordToTopicMap[token] = [(topicNum,topicTitle)]
        else:
            wordToTopicMap[token].append((topicNum,topicTitle))

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

        cleanTokens = specCharRegex.sub(" ", tweetText.lower(), count=0)
        tokens = set([wnl.lemmatize(x) for x in cleanTokens.split(" ")])
        print tokens

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

            item = {"topic":localTopic[0], "title": localTopic[1], "time":captureTime, "id":tweetId, "text":tweetText}
            outputRows.append(item)


outputDf = pd.DataFrame(outputRows)

outputDf.to_csv(outputPath, columns=["topic", "title", "time", "id", "text"], index=False)