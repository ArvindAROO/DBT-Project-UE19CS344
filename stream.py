import tweepy
from tweepy import Stream
from tweepy.streaming import Stream
from tweepy import OAuthHandler
import socket
import json

consumer_key='WEtaT25VUmF4SjJGd0ZFNU9PTk06MTpjaQ'
consumer_secret='OGtQ-p4f-7GxGoiMGUyl1d_jdEsIYwr-pwXx5e2VOvrakvgUhW'
access_token ='zgoAlf6Yio9fzY2MYk636Cl0D'
access_secret='lTrTubRhGKc0MUxcxRjMBAR2XCTV7p690zAKkQ5SKe3iEZpXtg'

class TweetsListener(Stream):
  # tweet object listens for the tweets
  def __init__(self,*args,csocket):
    super().__init__(*args)
    self.client_socket = csocket
  def on_data(self, data):
    try:  
      msg = json.loads( data )
      print("new message")
      # if tweet is longer than 140 characters
      if "extended_tweet" in msg:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['extended_tweet']['full_text']+"t_end")\
            .encode('utf-8'))         
        print(msg['extended_tweet']['full_text'])
      else:
        # add at the end of each tweet "t_end" 
        self.client_socket\
            .send(str(msg['text']+"t_end")\
            .encode('utf-8'))
        print(msg['text'])
      return True
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return True
  def on_error(self, status):
    print(status)
    return True
def sendData(c_socket, keyword):
  print('start sending data from Twitter to socket')
  # authentication based on the credentials
  auth = OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_secret)
  # start sending data from the Streaming API 
  #twitter_stream = Stream(auth, TweetsListener(c_socket))
  twitter_stream = TweetsListener(
        consumer_key, consumer_secret,
        access_token, access_secret,
        csocket=c_socket
    )
  twitter_stream.filter(track = keyword, languages=["en"])

if __name__ == "__main__":
    # server (local machine) creates listening socket
    s = socket.socket()
    host = "0.0.0.0"    
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    # server (local machine) listens for connections
    s.listen(4)
    print('socket is listening')
    # return the socket and the address on the other side of the connection (client side)
    c_socket, addr = s.accept()
    #print("socket" + c_socket)
    print("Received request from: " + str(addr))
    # select here the keyword for the tweet data
    sendData(c_socket, keyword = ['piano'])
