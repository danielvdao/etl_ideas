from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer, KafkaClient
import json

properties = open('properties.txt', 'r')
lines = properties.read().split('\n')
access_token = lines[0].split('=')[1].replace('"', '').strip()
access_token_secret = lines[1].split('=')[1].replace('"', '').strip()
consumer_key = lines[2].split('=')[1].replace('"', '').strip()
consumer_secret = lines[3].split('=')[1].replace('"', '').strip()


class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("trump", data.encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)


producer = KafkaProducer()
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)

stream.filter(track="donald trump")
