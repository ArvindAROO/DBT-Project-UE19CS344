import tweepy
import socket
import json



consumer_key=""
consumer_secret =""
access_token=""
access_secret=""

class tweepyClientWrapper(tweepy.Stream):

    def setCon(self,conn):
        self.conn = conn
    def on_data(self, data):
        try:
            tweetDict = json.loads(data)
            actualTweet=tweetDict['text']
            self.conn.send(bytes(actualTweet,'utf-8'))
        except Exception as e:
            print(e)
        return True

    def on_error(self, status):
        print(status)
        return True



s = socket.socket()
host = "localhost"
port = 9008
s.bind((host, port))
print('Socket is ready')

s.listen(4)
print('Socket listening')

c_socket, addr = s.accept()

print("client is " + str(addr))


tweepyClient = tweepyClientWrapper(consumer_key, consumer_secret, access_token, access_secret)
tweepyClient.setCon(c_socket)

tweepyClient.filter(track=['#RCB','#CSK','#MI','#SRH', '#elonmusk'])
