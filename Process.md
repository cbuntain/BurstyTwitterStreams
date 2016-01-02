# Process

1. Topic Parser
	- Lemmatize the topic name tokens
2. SPARK
3. Filter.py 
	- Assign tweets to topics based on tokens
4. Scenario + type runes
	- Scenario a - report no more than 10 tweets about a given topic in this moment
		- Type a: require at least two topic tokens for a tweet to match
		- Type b: require at least one topic tokens for a tweet to match
	- Scenario b - report now more than 100 tweets about a topic for the daily digest
		- Type a: require at least two topic tokens for a tweet to match
		- Type b: require at least one topic tokens for a tweet to match
