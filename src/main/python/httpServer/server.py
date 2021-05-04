#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
import requests
import json
import tweepy
import sys

from requests.auth import HTTPBasicAuth

elasticUrl = "http://aaa.bbb:9200"
indexName = "tweets"
dataType = "_doc"

user = "xxx"
passwd = "xxx"

CONSUMER_KEY=None
CONSUMER_SECRET=None
ACCESS_TOKEN=None
ACCESS_TOKEN_SECRET=None

def loadProperties(filepath, sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip('" \t')
                props[key] = value
    return props



def postData(tweet_json):

    tweet_meta = json.loads(tweet_json)

    # Skip tweets with no ID
    if "id" not in tweet_meta:
        return None

    # Set up API
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

    try:
        api = tweepy.API(auth, wait_on_rate_limit=True)

        # Test API
        status = api.get_status(tweet_meta["id"], tweet_mode="extended")
        tweet = status._json

        # Default values
        tweet["jaccardSim"] = 0.0
        tweet["tweetBurstCount"] = 0
        if "jaccardSim" in tweet:
            tweet["jaccardSim"] = tweet_meta["jaccardSim"]
            tweet["tweetBurstCount"] = tweet_meta["tweetBurstCount"]

        # update with the full tweet JSON
        tweet_json = json.dumps(tweet)

        # Debug
        text_field = None
        if "text" in tweet:
            text_field = tweet["text"]
        else:
            text_field = tweet["full_text"]
        logging.info(tweet["id_str"] + "-" + text_field)

        # Build the URL
        tweetId = tweet["id_str"]
        targetUrl = "{0}/{1}/{2}/{3}".\
            format(elasticUrl, indexName, dataType, tweetId)

        logging.info("Posting to ES URL:" + targetUrl)
        r = requests.put(
                targetUrl, 
                data=tweet_json, 
                headers={"Content-Type": "application/json"},
                auth=HTTPBasicAuth(user, passwd)
                )
        logging.info("Result: %d" % r.status_code)

    except Exception as e:
        logging.error("Error: " + str(e))
        

class S(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path), str(self.headers))
        self._set_response()
        self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    def do_POST(self):
        content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
        post_data = self.rfile.read(content_length) # <--- Gets the data itself
        logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                str(self.path), str(self.headers), post_data.decode('utf-8'))

        self._set_response()
        self.wfile.write("POST request for {}".format(self.path).encode('utf-8'))

        posted_data = post_data.decode('utf-8')
        postData(posted_data)

def run(server_class=HTTPServer, handler_class=S, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    logging.info('Starting httpd...\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:

        propFile = sys.argv[1]
        propDict = loadProperties(propFile)

        CONSUMER_KEY=propDict["oauth.consumerKey"]
        CONSUMER_SECRET=propDict["oauth.consumerSecret"]
        ACCESS_TOKEN=propDict["oauth.accessToken"]
        ACCESS_TOKEN_SECRET=propDict["oauth.accessTokenSecret"]

        run()
    else:
        run()
