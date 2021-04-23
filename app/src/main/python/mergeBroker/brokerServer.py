#!/usr/bin/python

import sys
import time
import string
import sqlite3
import requests
import BaseHTTPServer
import tweepy
import os
import smtplib
from email.mime.text import MIMEText

me = "cbuntain@coretx.net"

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

excludeSet = set(string.punctuation)
def tokenizer(tweetText):
    noPunctStr = ''.join([ch for ch in tweetText if ch not in excludeSet])
    return noPunctStr.lower().split(" ")

def makeHandler(dbConn, twitter, brokerUrl, simThreshold):
    dbCursor = dbConn.cursor()

    class BrokerHandler(BaseHTTPServer.BaseHTTPRequestHandler):

        # Handle GET requests
        def do_GET(self):

            print self.path
            if ( self.path == "/list" ):
                self.send_response(200)
                self.send_header('Content-Type','text/html; charset=utf-8')
                self.end_headers()

                self.wfile.write("<html><head><title>Submitted Tweets</title></head>")
                self.wfile.write("<body>")

                for row in dbCursor.execute('SELECT tweetid, tweet FROM tweet_info ORDER BY ROWID'):
                    tweetData = row[0].split("+")
                    topic = tweetData[0]
                    tweetId = tweetData[1]
                    tweetText = row[1]

                    lineStr = u"<p>%s - %s : %s</p>" % (topic, tweetId, tweetText)
                    self.wfile.write(lineStr.encode("utf8"))

                self.wfile.write("</body></html>")
            else:
                self.send_response(404)
                self.end_headers()


        # Handle POST requests
        def do_POST(self):

            self.send_response(204)
            self.send_header("Content-type", "text/html")
            self.end_headers()

            print "Received path:", self.path

            if ( self.path.startswith("/tweet") ):

                try:
                    # Format: POST /tweet/:topid/:tweetid/:clientid
                    postData = self.path.split("/")
                    topic = postData[2]
                    tweetId = postData[3]
                    uniqueTweetId = "%s+%s" % (topic, tweetId)

                    print "Tweet ID:", uniqueTweetId

                    dbCursor.execute('SELECT COUNT(*) FROM tweet_info WHERE tweetid=?', [uniqueTweetId])
                    matchCount = dbCursor.fetchone()[0]
                    print "How many times have we reported this tweet id?", matchCount

                    if ( matchCount == 0 ):
                        status = twitter.get_status(tweetId)
                        statusTokens = set(tokenizer(status.text))

                        maxSim = 0.0
                        for row in dbCursor.execute('SELECT tweetid, tweet FROM tweet_info'):
                            otherTweetText = row[1]
                            otherTokens = set(tokenizer(otherTweetText))

                            sim = float(len(statusTokens.intersection(otherTokens))) / float(len(statusTokens.union(otherTokens)))
                            maxSim = max(sim, maxSim)

                        print "Max Similarity:", maxSim

                        # Only insert in DB and push to broker if we are sufficiently dissimilar
                        if ( maxSim < simThreshold ):
                            dbCursor.execute('INSERT INTO tweet_info (path, tweetid, tweet) VALUES (?, ?, ?)', (self.path, uniqueTweetId, status.text))

                            populatedUrl = brokerUrl + self.path
                            print "Should send to:", populatedUrl

                            # send email about tweet
                            try:
                                emailMsg = MIMEText(u"New Tweet: %s - %s" % (uniqueTweetId, status.text), 'plain')
                                emailMsg['Subject'] = "New Tweet"
                                emailMsg['To'] = me
                                emailMsg['From'] = me
                                # Send the message via our own SMTP server, but don't include the
                                # envelope header.
                                s = smtplib.SMTP('localhost')
                                s.sendmail(me, [me], emailMsg.as_string())
                                s.quit()
                            except Exception as e:
                                print "Error sending email: {0}".format(e.message)

                            if ( not os.path.exists("skip_file") ):
                                resp = requests.post(populatedUrl)

                                print "Response Code:", resp.status_code
                                if ( resp.status_code != 204 ):
                                    print "Error!"
                                    print resp.headers
                                else:
                                    dbConn.commit()
                            else:
                                dbConn.commit()
                        else:
                            print "Tweet was too similar to others."

                    else:
                        print "Already submitted this tweet."
                except Exception as e:
                    raise e
                    # print "Error: {0}".format(e.message)
                    # pass

            else:
                print "Error. Malformed POST."

    return BrokerHandler


propFile = sys.argv[1]
propDict = loadProperties(propFile)
HOST_NAME = propDict["PERSONAL_BROKER_HOST"]
PORT_NUMBER = int(propDict["PERSONAL_BROKER_PORT"])
BROKER_URL = propDict["BROKER_URL"]

CONSUMER_KEY=propDict["CONSUMER_KEY"]
CONSUMER_SECRET=propDict["CONSUMER_SECRET"]
ACCESS_TOKEN=propDict["ACCESS_TOKEN"]
ACCESS_TOKEN_SECRET=propDict["ACCESS_TOKEN_SECRET"]

SIM_THRESHOLD=float(propDict["SIM_THRESHOLD"])

# Create the database for storing tweet info we submit
conn = sqlite3.connect("tweet_data.db")

# Create table
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS tweet_info (path TEXT, tweetid TEXT, tweet TEXT)''')
conn.commit()

# Connect to twitter
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

api = tweepy.API(auth)

# Test API
status = api.get_status("738418531520352258")
print status.text

# Create our HTTP server
server_class = BaseHTTPServer.HTTPServer
httpd = server_class((HOST_NAME, PORT_NUMBER), makeHandler(conn, api, BROKER_URL, SIM_THRESHOLD))
print time.asctime(), "Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER)

try:
    httpd.serve_forever()
except KeyboardInterrupt:
    pass

httpd.server_close()