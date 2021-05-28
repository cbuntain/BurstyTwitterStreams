#!/usr/bin/env python3
"""
Very simple HTTP server in python for logging requests
Usage::
    ./server.py [<port>]
"""
from http.server import BaseHTTPRequestHandler, HTTPServer
import logging
from elasticsearch import Elasticsearch
import json
import tweepy
import os
import argparse

indexName = os.environ.get('ES_INDEX')
dataType = "_doc"

user = os.environ.get('ES_USER')
passwd = os.environ.get('ES_PASSWOD')
elasticUrl = os.environ.get('ES_HOST')
port = os.environ.get('ES_PORT')
CONSUMER_KEY= os.environ.get('CONSUMER_KEY')
CONSUMER_SECRET=os.environ.get('CONSUMER_SECRET')
ACCESS_TOKEN=os.environ.get('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET=os.environ.get('ACCESS_TOKEN_SECRET')

ES = Elasticsearch(
            host=elasticUrl,
            http_auth=(user, passwd),
            scheme='http',
            port=port
        )



def argparser():
    """Parses CLI arguments
    :returns: parser

    """

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Load server arguments")
    parser.add_argument("--properties",
                        help="Property Dict")
    parser.add_argument("--port", help="Server Port", default=8080)
    args = parser.parse_args()

    return args


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
    # {"id": 759043035355312128, "id_str": "759043035355312128"}

    tweet_meta = json.loads(tweet_json)

    # Skip tweets with no ID
    if "id" not in tweet_meta:
        return None

    # Set up API
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    try:
        logging.info('Attempting to get twitter auth')
        redirect_url = auth.get_authorization_url()
    except tweepy.TweepError:
        logging.error('Error! Failed to get request token.')

    try:
        api = tweepy.API(auth, wait_on_rate_limit=True)

        # Test API
        status = api.get_status(tweet_meta["id"], tweet_mode="extended")
        tweet = status._json

        logging.info(f'Found tweet: {tweet}')

        # Default values
        logging.info('Setting default values')

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
        targetUrl = "{0}/{1}/{2}/{3}". \
            format(elasticUrl, indexName, dataType, tweetId)

        logging.info(f"Posting to ES URL: {targetUrl} with tweet: {tweet_json} "
                     f"and user: {user}, pwd: {passwd}"  )

        res = ES.index(index=indexName, id=tweetId, body=tweet)
        logging.info("Result: %s" % res)

    except Exception as e:
        logging.error("Error: " + str(e))


class S(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        logging.info("GET request,\nPath: %s\nHeaders:\n%s\n", str(self.path),
                     str(self.headers))
        self._set_response()
        self.wfile.write("GET request for {}".format(self.path).encode('utf-8'))

    def do_POST(self):
        content_length = int(
            self.headers['Content-Length'])  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself
        logging.info("POST request,\nPath: %s\nHeaders:\n%s\n\nBody:\n%s\n",
                     str(self.path), str(self.headers),
                     post_data.decode('utf-8'))

        self._set_response()
        self.wfile.write(
            "POST request for {}".format(self.path).encode('utf-8'))

        posted_data = post_data.decode('utf-8')
        logging.info(posted_data)
        postData(posted_data)


def run(server_class=HTTPServer, handler_class=S, port=8080):
    logging.basicConfig(level=logging.INFO)
    server_address = ('', int(port))
    httpd = server_class(server_address, handler_class)
    logging.info('Starting httpd...\n')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info('Stopping httpd...\n')


if __name__ == '__main__':
    args = argparser()
    if args.properties:

        propFile = args.properties
        propDict = loadProperties(propFile)

        CONSUMER_KEY = propDict["oauth.consumerKey"]
        CONSUMER_SECRET = propDict["oauth.consumerSecret"]
        ACCESS_TOKEN = propDict["oauth.accessToken"]
        ACCESS_TOKEN_SECRET = propDict["oauth.accessTokenSecret"]

        run(port=args.port)
    else:
        run(port=args.port)
