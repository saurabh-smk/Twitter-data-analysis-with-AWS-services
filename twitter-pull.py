'''
This script pulls data from Twitter API randonly and pushes to Kinsesis Firehose so to write in S3

To run this script Continuously use command: 
----
nohup python twitter-pull.py &
----
'''

# Import modules
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from pprint import pprint
import boto3
import json
import sys
import signal

# Twitter security credentials
ACCESS_TOKEN    = "123abc"
ACCESS_SECRET   = "123abc"
CONSUMER_KEY    = "123abc"
CONSUMER_SECRET = "123abc"

# Firehose stream name
FIREHOSE_STREAM = "sparkhose"

# Authenticate and initialize stream
oauth  = OAuth(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)
stream = TwitterStream(auth=oauth)
tweets = stream.statuses.sample()

# Setup Firehose
client = boto3.client('your-firehose-name')

# Start the loop to get the tweets.
for tweet in tweets :
    try :
        output   = tweet["user"]["screen_name"]      + ", " + \
                   tweet["place"]["name"]            + ", " + \
                   tweet["user"]["lang"]             + ", " + \
               str(tweet["user"]["statuses_count"])  + ", " + \
               str(tweet["user"]["followers_count"]) + ", " + \
               str(tweet["user"]["friends_count"])   + ", "
        hashtags = tweet["entities"]["hashtags"]
        hts = []
        if len(hashtags) == 0 :
            hts.append("None")
        else :
            for ht in hashtags :
                hts.append(str(ht["text"]))
        final = output + str(hts)
        # Final columns: screen_name, location, language, posts, followers, friends, [hashtags]
        print final
        response = client.put_record(
            DeliveryStreamName=FIREHOSE_STREAM,
            Record = {
                 'Data': final + '\n'
            }
        )
    except Exception :
        pass
        
        
# Author     : Rendy Oka
# Created    : March 16, 2016
