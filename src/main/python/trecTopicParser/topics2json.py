#!/usr/bin/python

from trec import Topics
import sys
import json
import string
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

if ( len(sys.argv) < 3 ):
    print "Usage: %s <trec_topics.txt> <output_file.json>" % (sys.argv[0])
    exit(1)

topicsFilePath = sys.argv[1]
outputFilePath = sys.argv[2]

topicsFile = open(topicsFilePath, "r")

topics = Topics(topicsFile)

enStops = stopwords.words("english")
wnl = WordNetLemmatizer()

topicList = []

for topic in topics.items():
    topicNum = topic[0]
    topicMap = topic[1]

    topicString = topicMap["title"].lower()
    topicTokens = [w.strip(string.punctuation) for w in topicString.split(" ")]

    newTokens = []
    for token in topicTokens:
        if token not in enStops:

            if ( len(token) > 2 ):
                newTokens.append(token)

            lemmatizedToken = wnl.lemmatize(token)

            if ( lemmatizedToken not in newTokens and len(lemmatizedToken) > 2 ):
                newTokens.append(lemmatizedToken)

    newTopicMap = {
        "num": topicNum,
        "title": topicString,
        "tokens": newTokens
    }
    topicList.append(newTopicMap)

    print topicNum, topicString, newTokens

with open(outputFilePath, "w") as outputFile:
    json.dump(topicList, outputFile, indent=4)

