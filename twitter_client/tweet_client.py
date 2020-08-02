from future.backports.socket import socket
from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import json
import  socket

class TweetsListner(StreamListener):
    def __init__(self,csocket):
        self.client_socket= csocket

    def on_data(self, data):
        try:
            # msg={"text":"","tag":"dmjsjd"}
            msg= json.loads(data)
            self.client_socket.send(msg['text'].encode('utf-8'))
            print("Check Data : ",msg['text'])
            return True
        except BaseException as e:
            print("Error on data : %s" % str(e))
        return True

    def on_error(self, status_code):
        print("error :",status_code)
        return True

CKey="AbvCUIBWzUMQmfM5Rf2E9DzHL"
CSec="aSQ0OfXiNsHTe2ZBkH3Wd7x9Evw3GWnZzUOxN5LjdWrc2ZFny3"
AToken="963024949-q2FjT9ocuhEiDcxbKUFjFlK3HDXnElWUXsgwwFYB"
ASec="79oPKKPgyJfbE5r217jffq5VNyrOJYSdAir0h103Mcaal"

def sendData(c):
    auth= OAuthHandler(CKey,CSec)
    auth.set_access_token(AToken,ASec)
    twitter_stream=Stream(auth,TweetsListner(csocket=c))
    twitter_stream.filter(languages=["en"],track=['ram','java','python'])



if __name__ == '__main__':
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    host="localhost"
    port=5656
    s.bind((host,port))
    s.listen(5)
    print("Listenning spark ")
    c,add= s.accept()
    print("Recevied from ",c,add)
    sendData(c)