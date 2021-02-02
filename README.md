# Code for TREC 2017 Real-Time Summarization Task

This code uses Spark Streaming to identify interesting tokens related to some given set of topics. Tweets containing these tokens are then saved to a file.

Operates by first splitting Twitter stream by topic using Lucene and then identifying bursty tokens in those topic streams.
