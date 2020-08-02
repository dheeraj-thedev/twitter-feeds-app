import json

import  socket
import tweepy
from tweepy import StreamListener, OAuthHandler, Stream

consumer_key = "AbvCUIBWzUMQmfM5Rf2E9DzHL"
consumer_secret = "aSQ0OfXiNsHTe2ZBkH3Wd7x9Evw3GWnZzUOxN5LjdWrc2ZFny3"
access_token = "963024949-q2FjT9ocuhEiDcxbKUFjFlK3HDXnElWUXsgwwFYB"
access_token_secret = "79oPKKPgyJfbE5r217jffq5VNyrOJYSdAir0h103Mcaal"
# Creating the authentication object



class TweetsListner(StreamListener):
    def __init__(self,csocket):
        self.client_socket= csocket

    def on_data(self, data):
        try:
            # msg={"text":"","tag":"dmjsjd"}
            # msg= json.loads(data)
            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            # Setting your access token and secret
            auth.set_access_token(access_token, access_token_secret)
            # Creating the API object while passing in auth information
            api = tweepy.API(auth)
            # Using the API object to get tweets from your timeline, and storing it in a variable called public_tweets
            public_tweets = api.home_timeline()
            # foreach through all tweets pulled
            for tweet in public_tweets:
                # printing the text stored inside the tweet object
                print(tweet.text)
                self.client_socket.send(tweet.encode('utf-8'))
                print("Check Data : ",tweet)
            return True
        except BaseException as e:
            print("Error on data : %s" % str(e))
        return True

    def on_error(self, status_code):
        print("error :",status_code)
        return True
def sendData(c_socket):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    twitter_stream = Stream(auth, TweetsListner(csocket=c))

if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host = "localhost"
    port = 5659
    s.bind((host, port))
    s.listen(5)
    print("Listenning spark ")
    c, add = s.accept()
    print("Recevied from ", c, add)
    sendData(c)