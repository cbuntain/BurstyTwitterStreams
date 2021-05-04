from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json
import re
import argparse
from pathlib import Path

api_key =  ""
api_secret =  ""
access_token = ""
access_token_secret = ""

topic_name = "cv19"

parser = argparse.ArgumentParser()
parser.add_argument("file_path", type=Path)
args = parser.parse_args()

print("Starting stream listener")
words = []

with open(args.file_path,"r",encoding='utf-8') as f:
    words = f.read().split(',')

if words != None:
    print("\tListening for key words:",words)

# Surrounding the UK
ukloc = [-6.15234375,49.809631563563094,1.9335937499999998,61.05828537037916]

followers = [
        "neilellis",
        "SkyNews",
        "Orgetorix",
        "BorisJohnson",
        "piersmorgan",
        "DHSCgovuk",
        "halfeatenmind",
        "SkyNewsBreak",
        "Mancman10",
        "Independent",
        "10DowningStreet",
        "MattHancock",
        "Lance63",
        "Hillchaser",
        "BBCNews",
        "Angie_RejoinEU",
        "davidh7426",
        "davi326",
        "robalexander001",
        "evilbluebird",
        "BBCBreaking",
        "fitzfun2011",
        "TheXIIGate",
        "SalfordCouncil",
        "mikewarburton",
        "Paulhaider74",
        "Moonbootica",
        "RencapMan",
]
followers_str = ",".join(followers)

trackers = ["@%s" % x for x in followers]
trackers_str = ",".join(trackers)

if ukloc != None:
    print("\tListening for coordinates:",ukloc)

if followers_str != None:
    print("\tUsers:", followers_str)
    print("\tTrack:", trackers_str)

class StdOutListener(StreamListener):
    def on_data(self, data):
        json_ = json.loads(data) 
        if "id_str" not in json_:
            print("Odd Tweet:", data)
            return True

        print("Tweet:", json_["id_str"])
        producer.send(topic_name, data.encode("utf8"))

        return True
#        if True or any(word in json_["text"] for word in words):
#            producer.send(topic_name, data)
#            return True

    def on_error(self, status):
        print ("Error Status: ",status)

producer = KafkaProducer(bootstrap_servers='localhost:19092')
l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)

stream = Stream(auth, l)
stream.filter(locations=ukloc,track=trackers)
